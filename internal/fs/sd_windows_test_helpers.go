//go:build windows

package fs

import (
	"os/user"
	"testing"

	"github.com/otuschhoff/vaultic/internal/errors"
	"github.com/otuschhoff/vaultic/internal/test"
	"golang.org/x/sys/windows"
)

var (
	testFileSDs = []string{("AQAUvBQAAAAwAAAAAAAAAEwAAAABBQAAAAAABRUAAACIn1iuVqCC6sy9JqvqAwAAAQUAAAAAAAUV" +
		"AAAAiJ9YrlaggurMvSarAQIAAAIAfAAEAAAAAAAkAKkAEgABBQAAAAAABRUAAACIn1iuVqCC6sy9" +
		"JqvtAwAAABAUAP8BHwABAQAAAAAABRIAAAAAEBgA/wEfAAECAAAAAAAFIAAAACACAAAAECQA/wEf" +
		"AAEFAAAAAAAFFQAAAIifWK5WoILqzL0mq+oDAAA="),
		("AQAUvBQAAAAwAAAAAAAAAEwAAAABBQAAAAAABRUAAACIn1iuVqCC6sy9JqvqAwAAAQUAAAAAAAUV" +
			"AAAAiJ9YrlaggurMvSarAQIAAAIAyAAHAAAAAAAUAKkAEgABAQAAAAAABQcAAAAAABQAiQASAAEB" +
			"AAAAAAAFBwAAAAAAJACpABIAAQUAAAAAAAUVAAAAiJ9YrlaggurMvSar7QMAAAAAJAC/ARMAAQUA" +
			"AAAAAAUVAAAAiJ9YrlaggurMvSar6gMAAAAAFAD/AR8AAQEAAAAAAAUSAAAAAAAYAP8BHwABAgAA" +
			"AAAABSAAAAAgAgAAAAAkAP8BHwABBQAAAAAABRUAAACIn1iuVqCC6sy9JqvqAwAA"),
		("AQAUvBQAAAAwAAAA7AAAAEwAAAABBQAAAAAABRUAAAAvr7t03PyHGk2FokNHCAAAAQUAAAAAAAUV" +
			"AAAAiJ9YrlaggurMvSarAQIAAAIAoAAFAAAAAAAkAP8BHwABBQAAAAAABRUAAAAvr7t03PyHGk2F" +
			"okNHCAAAAAAkAKkAEgABBQAAAAAABRUAAACIn1iuVqCC6sy9JqvtAwAAABAUAP8BHwABAQAAAAAA" +
			"BRIAAAAAEBgA/wEfAAECAAAAAAAFIAAAACACAAAAECQA/wEfAAEFAAAAAAAFFQAAAIifWK5WoILq" +
			"zL0mq+oDAAACAHQAAwAAAAKAJAC/AQIAAQUAAAAAAAUVAAAAL6+7dNz8hxpNhaJDtgQAAALAJAC/" +
			"AQMAAQUAAAAAAAUVAAAAL6+7dNz8hxpNhaJDPgkAAAJAJAD/AQ8AAQUAAAAAAAUVAAAAL6+7dNz8" +
			"hxpNhaJDtQQAAA=="),
	}
	testDirSDs = []string{("AQAUvBQAAAAwAAAAAAAAAEwAAAABBQAAAAAABRUAAACIn1iuVqCC6sy9JqvqAwAAAQUAAAAAAAUV" +
		"AAAAiJ9YrlaggurMvSarAQIAAAIAfAAEAAAAAAAkAKkAEgABBQAAAAAABRUAAACIn1iuVqCC6sy9" +
		"JqvtAwAAABMUAP8BHwABAQAAAAAABRIAAAAAExgA/wEfAAECAAAAAAAFIAAAACACAAAAEyQA/wEf" +
		"AAEFAAAAAAAFFQAAAIifWK5WoILqzL0mq+oDAAA="),
		("AQAUvBQAAAAwAAAAAAAAAEwAAAABBQAAAAAABRUAAACIn1iuVqCC6sy9JqvqAwAAAQUAAAAAAAUV" +
			"AAAAiJ9YrlaggurMvSarAQIAAAIA3AAIAAAAAAIUAKkAEgABAQAAAAAABQcAAAAAAxQAiQASAAEB" +
			"AAAAAAAFBwAAAAAAJACpABIAAQUAAAAAAAUVAAAAiJ9YrlaggurMvSar7QMAAAAAJAC/ARMAAQUA" +
			"AAAAAAUVAAAAiJ9YrlaggurMvSar6gMAAAALFAC/ARMAAQEAAAAAAAMAAAAAABMUAP8BHwABAQAA" +
			"AAAABRIAAAAAExgA/wEfAAECAAAAAAAFIAAAACACAAAAEyQA/wEfAAEFAAAAAAAFFQAAAIifWK5W" +
			"oILqzL0mq+oDAAA="),
		("AQAUvBQAAAAwAAAA7AAAAEwAAAABBQAAAAAABRUAAAAvr7t03PyHGk2FokNHCAAAAQUAAAAAAAUV" +
			"AAAAiJ9YrlaggurMvSarAQIAAAIAoAAFAAAAAAAkAP8BHwABBQAAAAAABRUAAAAvr7t03PyHGk2F" +
			"okNHCAAAAAAkAKkAEgABBQAAAAAABRUAAACIn1iuVqCC6sy9JqvtAwAAABMUAP8BHwABAQAAAAAA" +
			"BRIAAAAAExgA/wEfAAECAAAAAAAFIAAAACACAAAAEyQA/wEfAAEFAAAAAAAFFQAAAIifWK5WoILq" +
			"zL0mq+oDAAACAHQAAwAAAAKAJAC/AQIAAQUAAAAAAAUVAAAAL6+7dNz8hxpNhaJDtgQAAALAJAC/" +
			"AQMAAQUAAAAAAAUVAAAAL6+7dNz8hxpNhaJDPgkAAAJAJAD/AQ8AAQUAAAAAAAUVAAAAL6+7dNz8" +
			"hxpNhaJDtQQAAA=="),
	}
)

// isAdmin checks if current user is an administrator.
func isAdmin() (isAdmin bool, err error) {
	var sid *windows.SID
	err = windows.AllocateAndInitializeSid(&windows.SECURITY_NT_AUTHORITY, 2, windows.SECURITY_BUILTIN_DOMAIN_RID, windows.DOMAIN_ALIAS_RID_ADMINS,
		0, 0, 0, 0, 0, 0, &sid)
	if err != nil {
		return false, errors.Errorf("sid error: %w", err)
	}
	windows.GetCurrentProcessToken()
	token := windows.Token(0)
	member, err := token.IsMember(sid)
	if err != nil {
		return false, errors.Errorf("token membership error: %w", err)
	}
	return member, nil
}

// compareSecurityDescriptors runs tests for comparing 2 security descriptors in []byte format.
func compareSecurityDescriptors(t *testing.T, testPath string, sdInputBytes, sdOutputBytes []byte) {
	sdInput, err := securityDescriptorBytesToStruct(sdInputBytes)
	test.OK(t, errors.Wrapf(err, "Error converting SD to struct for: %s", testPath))

	sdOutput, err := securityDescriptorBytesToStruct(sdOutputBytes)
	test.OK(t, errors.Wrapf(err, "Error converting SD to struct for: %s", testPath))

	isAdmin, err := isAdmin()
	test.OK(t, errors.Wrapf(err, "Error checking if user is admin: %s", testPath))

	var ownerExpected *windows.SID
	var defaultedOwnerExpected bool
	var groupExpected *windows.SID
	var defaultedGroupExpected bool
	var daclExpected *windows.ACL
	var defaultedDaclExpected bool
	var saclExpected *windows.ACL
	var defaultedSaclExpected bool

	// The Dacl is set correctly whether or not application is running as admin.
	daclExpected, defaultedDaclExpected, err = sdInput.DACL()
	test.OK(t, errors.Wrapf(err, "Error getting input dacl for: %s", testPath))

	if isAdmin {
		// If application is running as admin, all sd values including owner, group, dacl, sacl are set correctly during restore.
		// Hence we will use the input values for comparison with the output values.
		ownerExpected, defaultedOwnerExpected, err = sdInput.Owner()
		test.OK(t, errors.Wrapf(err, "Error getting input owner for: %s", testPath))
		groupExpected, defaultedGroupExpected, err = sdInput.Group()
		test.OK(t, errors.Wrapf(err, "Error getting input group for: %s", testPath))
		saclExpected, defaultedSaclExpected, err = sdInput.SACL()
		test.OK(t, errors.Wrapf(err, "Error getting input sacl for: %s", testPath))
	} else {
		// If application is not running as admin, owner and group are set as current user's SID/GID during restore and sacl is empty.
		// Get the current user
		user, err := user.Current()
		test.OK(t, errors.Wrapf(err, "Could not get current user for: %s", testPath))
		// Get current user's SID
		currentUserSID, err := windows.StringToSid(user.Uid)
		test.OK(t, errors.Wrapf(err, "Error getting output group for: %s", testPath))
		// Get current user's Group SID
		currentGroupSID, err := windows.StringToSid(user.Gid)
		test.OK(t, errors.Wrapf(err, "Error getting output group for: %s", testPath))

		// Set owner and group as current user's SID and GID during restore.
		ownerExpected = currentUserSID
		defaultedOwnerExpected = false
		groupExpected = currentGroupSID
		defaultedGroupExpected = false

		// If application is not running as admin, SACL is returned empty.
		saclExpected = nil
		defaultedSaclExpected = false
	}
	// Now do all the comparisons
	// Get owner SID from output file
	ownerOut, defaultedOwnerOut, err := sdOutput.Owner()
	test.OK(t, errors.Wrapf(err, "Error getting output owner for: %s", testPath))
	// Compare owner SIDs. We must use the Equals method for comparison as a syscall is made for comparing SIDs.
	test.Assert(t, ownerExpected.Equals(ownerOut), ("Owner from SDs read from test path don't match: %s, " +
		"cur:%s, exp: %s"), testPath, ownerExpected.String(), ownerOut.String())
	test.Equals(t, defaultedOwnerExpected, defaultedOwnerOut, "Defaulted for owner from SDs read from test path don't match: %s", testPath)

	// Get group SID from output file
	groupOut, defaultedGroupOut, err := sdOutput.Group()
	test.OK(t, errors.Wrapf(err, "Error getting output group for: %s", testPath))
	// Compare group SIDs. We must use the Equals method for comparison as a syscall is made for comparing SIDs.
	test.Assert(t, groupExpected.Equals(groupOut), ("Group from SDs read from test path don't match: %s, " +
		"cur:%s, exp: %s"), testPath, groupExpected.String(), groupOut.String())
	test.Equals(t, defaultedGroupExpected, defaultedGroupOut, "Defaulted for group from SDs read from test path don't match: %s", testPath)

	// Get dacl from output file
	daclOut, defaultedDaclOut, err := sdOutput.DACL()
	test.OK(t, errors.Wrapf(err, "Error getting output dacl for: %s", testPath))
	// Compare dacls
	test.Equals(t, daclExpected, daclOut, "DACL from SDs read from test path don't match: %s", testPath)
	test.Equals(t, defaultedDaclExpected, defaultedDaclOut, "Defaulted for DACL from SDs read from test path don't match: %s", testPath)

	// Get sacl from output file
	saclOut, defaultedSaclOut, err := sdOutput.SACL()
	test.OK(t, errors.Wrapf(err, "Error getting output sacl for: %s", testPath))
	// Compare sacls
	test.Equals(t, saclExpected, saclOut, "DACL from SDs read from test path don't match: %s", testPath)
	test.Equals(t, defaultedSaclExpected, defaultedSaclOut, "Defaulted for SACL from SDs read from test path don't match: %s", testPath)
}
