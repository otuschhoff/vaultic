package broker

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type awsKMSAPI interface {
	Decrypt(context.Context, *kms.DecryptInput, ...func(*kms.Options)) (*kms.DecryptOutput, error)
	DescribeKey(context.Context, *kms.DescribeKeyInput, ...func(*kms.Options)) (*kms.DescribeKeyOutput, error)
}

type awsSTSAPI interface {
	GetCallerIdentity(context.Context, *sts.GetCallerIdentityInput, ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error)
}

type AWSKMSUnwrapper struct {
	kms awsKMSAPI
	sts awsSTSAPI
}

func NewAWSKMSUnwrapper(ctx context.Context) (*AWSKMSUnwrapper, error) {
	configuration, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load AWS custodian identity: %w", err)
	}
	return &AWSKMSUnwrapper{kms: kms.NewFromConfig(configuration), sts: sts.NewFromConfig(configuration)}, nil
}

func (unwrapper *AWSKMSUnwrapper) UnwrapMember(ctx context.Context, member ExternalMemberContext, ciphertext []byte) ([]byte, VerifiedPrincipal, error) {
	account, err := validateAWSKMSKeyReference(member.KeyReference)
	if err != nil {
		return nil, VerifiedPrincipal{}, err
	}
	if (member.Provider != "aws-kms" && member.Provider != "aws-cloudhsm") || len(ciphertext) == 0 {
		return nil, VerifiedPrincipal{}, errors.New("invalid AWS KMS member request")
	}
	identity, err := unwrapper.sts.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return nil, VerifiedPrincipal{}, fmt.Errorf("AWS STS GetCallerIdentity: %w", err)
	}
	if aws.ToString(identity.Account) != account || aws.ToString(identity.Arn) == "" {
		return nil, VerifiedPrincipal{}, errors.New("AWS caller identity does not match KMS key account")
	}
	principal, err := stableAWSPrincipalARN(aws.ToString(identity.Arn), account)
	if err != nil {
		return nil, VerifiedPrincipal{}, err
	}
	if member.Provider == "aws-cloudhsm" {
		description, describeErr := unwrapper.kms.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: aws.String(member.KeyReference)})
		if describeErr != nil {
			return nil, VerifiedPrincipal{}, fmt.Errorf("AWS KMS DescribeKey: %w", describeErr)
		}
		if description.KeyMetadata == nil || description.KeyMetadata.Origin != types.OriginTypeAwsCloudhsm ||
			aws.ToString(description.KeyMetadata.CustomKeyStoreId) == "" {
			return nil, VerifiedPrincipal{}, errors.New("AWS CloudHSM member key is not backed by a CloudHSM custom key store")
		}
	}
	output, err := unwrapper.kms.Decrypt(ctx, &kms.DecryptInput{
		KeyId:          aws.String(member.KeyReference),
		CiphertextBlob: ciphertext,
		EncryptionContext: map[string]string{
			"vaultic:repository":  member.RepositoryID,
			"vaultic:slot":        member.MemberID,
			"vaultic:dek-version": strconv.FormatUint(uint64(member.RootKeyVersion), 10),
			"vaultic:purpose":     member.Purpose,
		},
	})
	if err != nil {
		return nil, VerifiedPrincipal{}, fmt.Errorf("AWS KMS Decrypt: %w", err)
	}
	if len(output.Plaintext) == 0 {
		return nil, VerifiedPrincipal{}, errors.New("AWS KMS returned empty plaintext")
	}
	return output.Plaintext, VerifiedPrincipal{Authority: "aws-iam", TenantAccountOrProject: account, ImmutablePrincipalID: principal}, nil
}

func stableAWSPrincipalARN(value, account string) (string, error) {
	parsed, err := arn.Parse(value)
	if err != nil || parsed.AccountID != account || parsed.Partition == "" {
		return "", errors.New("AWS caller identity has an invalid ARN")
	}
	if parsed.Service == "iam" && (strings.HasPrefix(parsed.Resource, "role/") || strings.HasPrefix(parsed.Resource, "user/")) {
		return parsed.String(), nil
	}
	if parsed.Service == "sts" && strings.HasPrefix(parsed.Resource, "assumed-role/") {
		roleAndSession := strings.TrimPrefix(parsed.Resource, "assumed-role/")
		separator := strings.LastIndexByte(roleAndSession, '/')
		if separator <= 0 || separator == len(roleAndSession)-1 {
			return "", errors.New("AWS assumed-role identity lacks a role or session name")
		}
		return arn.ARN{Partition: parsed.Partition, Service: "iam", AccountID: account, Resource: "role/" + roleAndSession[:separator]}.String(), nil
	}
	return "", errors.New("AWS caller identity must be an IAM user or role")
}

func validateAWSKMSKeyReference(reference string) (string, error) {
	parts := strings.Split(reference, ":")
	if len(parts) != 6 || parts[0] != "arn" || parts[1] == "" || parts[2] != "kms" || parts[3] == "" || len(parts[4]) != 12 ||
		!strings.HasPrefix(parts[5], "key/") ||
		len(parts[5]) <= len("key/") {
		return "", errors.New("AWS KMS key reference must be a full key ARN")
	}
	for _, character := range parts[4] {
		if character < '0' || character > '9' {
			return "", errors.New("AWS KMS key ARN has an invalid account ID")
		}
	}
	return parts[4], nil
}
