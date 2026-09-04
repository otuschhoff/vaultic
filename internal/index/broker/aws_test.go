package broker

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type kmsFunc func(context.Context, *kms.DecryptInput) (*kms.DecryptOutput, error)

func (function kmsFunc) Decrypt(
	ctx context.Context,
	input *kms.DecryptInput,
	_ ...func(*kms.Options),
) (*kms.DecryptOutput, error) {
	return function(ctx, input)
}

func (function kmsFunc) DescribeKey(
	context.Context,
	*kms.DescribeKeyInput,
	...func(*kms.Options),
) (*kms.DescribeKeyOutput, error) {
	return nil, errors.New("unexpected DescribeKey")
}

type cloudHSMKMS struct{ kmsFunc }

func (client cloudHSMKMS) DescribeKey(
	context.Context,
	*kms.DescribeKeyInput,
	...func(*kms.Options),
) (*kms.DescribeKeyOutput, error) {
	return &kms.DescribeKeyOutput{
		KeyMetadata: &types.KeyMetadata{CustomKeyStoreId: aws.String("cks-a"), Origin: types.OriginTypeAwsCloudhsm},
	}, nil
}

type stsFunc func(context.Context, *sts.GetCallerIdentityInput) (*sts.GetCallerIdentityOutput, error)

func (function stsFunc) GetCallerIdentity(
	ctx context.Context,
	input *sts.GetCallerIdentityInput,
	_ ...func(*sts.Options),
) (*sts.GetCallerIdentityOutput, error) {
	return function(ctx, input)
}

func TestAWSKMSUnwrapperNormalizesAssumedRoleAndVerifiesCloudHSM(t *testing.T) {
	unwrapper := &AWSKMSUnwrapper{
		sts: stsFunc(func(context.Context, *sts.GetCallerIdentityInput) (*sts.GetCallerIdentityOutput, error) {
			return &sts.GetCallerIdentityOutput{
				Account: aws.String("123456789012"),
				Arn:     aws.String("arn:aws:sts::123456789012:assumed-role/team/custodian-a/login-42"),
			}, nil
		}),
		kms: cloudHSMKMS{kmsFunc: func(context.Context, *kms.DecryptInput) (*kms.DecryptOutput, error) {
			return &kms.DecryptOutput{Plaintext: []byte("wrapped-share")}, nil
		}},
	}
	_, principal, err := unwrapper.UnwrapMember(
		context.Background(),
		ExternalMemberContext{
			RepositoryID:   "repo-a",
			RootKeyVersion: 1,
			MemberID:       "alice",
			Provider:       "aws-cloudhsm",
			KeyReference:   "arn:aws:kms:us-east-1:123456789012:key/key-a",
			Purpose:        "purpose-a",
		},
		[]byte("ciphertext"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if principal.ImmutablePrincipalID != "arn:aws:iam::123456789012:role/team/custodian-a" {
		t.Fatalf("unstable AWS principal: %q", principal.ImmutablePrincipalID)
	}
	if _, err := stableAWSPrincipalARN("arn:aws:sts::123456789012:federated-user/alice", "123456789012"); err == nil {
		t.Fatal("unstable federated-user principal accepted")
	}
}

func TestAWSKMSUnwrapperBindsContextAndCallerIdentity(t *testing.T) {
	unwrapper := &AWSKMSUnwrapper{
		sts: stsFunc(func(context.Context, *sts.GetCallerIdentityInput) (*sts.GetCallerIdentityOutput, error) {
			return &sts.GetCallerIdentityOutput{
				Account: aws.String("123456789012"),
				Arn:     aws.String("arn:aws:iam::123456789012:role/custodian-a"),
			}, nil
		}),
		kms: kmsFunc(func(_ context.Context, input *kms.DecryptInput) (*kms.DecryptOutput, error) {
			if aws.ToString(input.KeyId) != "arn:aws:kms:us-east-1:123456789012:key/key-a" ||
				input.EncryptionContext["vaultic:repository"] != "repo-a" ||
				input.EncryptionContext["vaultic:slot"] != "alice" ||
				input.EncryptionContext["vaultic:dek-version"] != "1" ||
				input.EncryptionContext["vaultic:purpose"] != "purpose-a" {
				t.Fatalf("unexpected AWS KMS context: %+v", input)
			}
			return &kms.DecryptOutput{Plaintext: []byte("wrapped-share")}, nil
		}),
	}
	plaintext, principal, err := unwrapper.UnwrapMember(
		context.Background(),
		ExternalMemberContext{
			RepositoryID:   "repo-a",
			RootKeyVersion: 1,
			MemberID:       "alice",
			Provider:       "aws-kms",
			KeyReference:   "arn:aws:kms:us-east-1:123456789012:key/key-a",
			Purpose:        "purpose-a",
		},
		[]byte("ciphertext"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(plaintext) != "wrapped-share" || principal.Authority != "aws-iam" ||
		principal.TenantAccountOrProject != "123456789012" ||
		principal.ImmutablePrincipalID != "arn:aws:iam::123456789012:role/custodian-a" {
		t.Fatalf("unexpected AWS result: %q %+v", plaintext, principal)
	}
}

func TestAWSKMSUnwrapperFailsClosed(t *testing.T) {
	if _, err := validateAWSKMSKeyReference("alias/not-a-key-arn"); err == nil {
		t.Fatal("AWS alias accepted instead of immutable key ARN")
	}
	unwrapper := &AWSKMSUnwrapper{
		sts: stsFunc(func(context.Context, *sts.GetCallerIdentityInput) (*sts.GetCallerIdentityOutput, error) {
			return &sts.GetCallerIdentityOutput{
				Account: aws.String("999999999999"),
				Arn:     aws.String("arn:aws:iam::999999999999:role/wrong"),
			}, nil
		}),
		kms: kmsFunc(func(context.Context, *kms.DecryptInput) (*kms.DecryptOutput, error) {
			return nil, errors.New("denied")
		}),
	}
	if _,
		_,
		err := unwrapper.UnwrapMember(context.Background(),
		ExternalMemberContext{Provider: "aws-kms",
			KeyReference: ("arn:aws:kms:us-east-1:123456789012:key/key-a")},
		[]byte("ciphertext")); err == nil {
		t.Fatal("AWS account mismatch accepted")
	}
}
