//go:build windows

package fs

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/go-ole/go-ole"
	"github.com/otuschhoff/vaultic/internal/errors"
	"golang.org/x/sys/windows"
)

// IVSSAdminVTable is the vtable for IVSSAdmin.
// nolint:structcheck
type IVSSAdminVTable struct {
	ole.IUnknownVtbl
	registerProvider            uintptr
	unregisterProvider          uintptr
	queryProviders              uintptr
	abortAllSnapshotsInProgress uintptr
}

// getVTable returns the vtable for IVSSAdmin.
func (vssAdmin *IVSSAdmin) getVTable() *IVSSAdminVTable {
	return (*IVSSAdminVTable)(unsafe.Pointer(vssAdmin.RawVTable))
}

// QueryProviders calls the equivalent VSS api.
func (vssAdmin *IVSSAdmin) QueryProviders() (*IVssEnumObject, error) {
	var enum *IVssEnumObject

	result, _, _ := syscall.Syscall(vssAdmin.getVTable().queryProviders, 2,
		uintptr(unsafe.Pointer(vssAdmin)), uintptr(unsafe.Pointer(&enum)), 0)

	return enum, newVssErrorIfResultNotOK("QueryProviders() failed", HRESULT(result))
}

// IVssEnumObject VSS api interface.
type IVssEnumObject struct {
	ole.IUnknown
}

// IVssEnumObjectVTable is the vtable for IVssEnumObject.
// nolint:structcheck
type IVssEnumObjectVTable struct {
	ole.IUnknownVtbl
	next  uintptr
	skip  uintptr
	reset uintptr
	clone uintptr
}

// getVTable returns the vtable for IVssEnumObject.
func (vssEnum *IVssEnumObject) getVTable() *IVssEnumObjectVTable {
	return (*IVssEnumObjectVTable)(unsafe.Pointer(vssEnum.RawVTable))
}

// Next calls the equivalent VSS api.
func (vssEnum *IVssEnumObject) Next(count uint, props unsafe.Pointer) (uint, error) {
	var fetched uint32
	result, _, _ := syscall.Syscall6(vssEnum.getVTable().next, 4,
		uintptr(unsafe.Pointer(vssEnum)), uintptr(count), uintptr(props),
		uintptr(unsafe.Pointer(&fetched)), 0, 0)
	if HRESULT(result) == S_FALSE {
		return uint(fetched), nil
	}

	return uint(fetched), newVssErrorIfResultNotOK("Next() failed", HRESULT(result))
}

// mountPoint wraps all information of a snapshot of a mountpoint on a volume.
type mountPoint struct {
	isSnapshotted        bool
	snapshotSetID        ole.GUID
	snapshotProperties   vssSnapshotProperties
	snapshotDeviceObject string
}

// IsSnapshotted is true if this mount point was snapshotted successfully.
func (p *mountPoint) IsSnapshotted() bool {
	return p.isSnapshotted
}

// GetSnapshotDeviceObject returns root path to access the snapshot files and folders.
func (p *mountPoint) GetSnapshotDeviceObject() string {
	return p.snapshotDeviceObject
}

// vssSnapshot wraps windows volume shadow copy api (vss) via a simple
// interface to create and delete a vss snapshot.
type vssSnapshot struct {
	iVssBackupComponents *IVssBackupComponents
	snapshotID           ole.GUID
	snapshotProperties   vssSnapshotProperties
	snapshotDeviceObject string
	mountPointInfo       map[string]mountPoint
	timeout              time.Duration
}

// GetSnapshotDeviceObject returns root path to access the snapshot files
// and folders.
func (p *vssSnapshot) GetSnapshotDeviceObject() string {
	return p.snapshotDeviceObject
}

// initializeVssCOMInterface initialize an instance of the VSS COM api
func initializeVssCOMInterface() (*ole.IUnknown, error) {
	vssInstance, err := loadIVssBackupComponentsConstructor()
	if err != nil {
		return nil, err
	}

	// ensure COM is initialized before use
	if err = ole.CoInitializeEx(0, ole.COINIT_MULTITHREADED); err != nil {
		// CoInitializeEx returns S_FALSE if COM is already initialized
		if oleErr, ok := err.(*ole.OleError); !ok || HRESULT(oleErr.Code()) != S_FALSE {
			return nil, err
		}
	}

	// initialize COM security for VSS, this can't be called more then once

	// Allowing all processes to perform incoming COM calls is not necessarily a security weakness.
	// A requester acting as a COM server, like all other COM servers, always retains the option to authorize its clients on every COM method implemented in its process.
	//
	// Note that internal COM callbacks implemented by VSS are secured by default.
	// Reference: https://learn.microsoft.com/en-us/windows/win32/vss/security-considerations-for-requestors#:~:text=Allowing%20all%20processes,secured%20by%20default.

	if err = ole.CoInitializeSecurity(
		-1,   // Default COM authentication service
		6,    // RPC_C_AUTHN_LEVEL_PKT_PRIVACY
		3,    // RPC_C_IMP_LEVEL_IMPERSONATE
		0x20, // EOAC_STATIC_CLOAKING
	); err != nil {
		// TODO warn for expected event logs for VSS IVssWriterCallback failure
		return nil, newVssError(
			"Failed to initialize security for VSS request",
			HRESULT(err.(*ole.OleError).Code()))
	}

	var oleIUnknown *ole.IUnknown
	result, _, _ := vssInstance.Call(uintptr(unsafe.Pointer(&oleIUnknown)))
	hresult := HRESULT(result)

	switch hresult {
	case S_OK:
	case E_ACCESSDENIED:
		return oleIUnknown, newVssError(
			"The caller does not have sufficient backup privileges or is not an administrator",
			hresult)
	default:
		return oleIUnknown, newVssError("Failed to create VSS instance", hresult)
	}

	if oleIUnknown == nil {
		return nil, newVssError("Failed to initialize COM interface", hresult)
	}

	return oleIUnknown, nil
}

// HasSufficientPrivilegesForVSS returns nil if the user is allowed to use VSS.
func HasSufficientPrivilegesForVSS() error {
	oleIUnknown, err := initializeVssCOMInterface()
	if oleIUnknown != nil {
		oleIUnknown.Release()
	}

	return err
}

// getVolumeNameForVolumeMountPoint add trailing backslash to input parameter
// and calls the equivalent windows api.
func getVolumeNameForVolumeMountPoint(mountPoint string) (string, error) {
	if mountPoint != "" && mountPoint[len(mountPoint)-1] != filepath.Separator {
		mountPoint += string(filepath.Separator)
	}

	mountPointPointer, err := syscall.UTF16PtrFromString(mountPoint)
	if err != nil {
		return mountPoint, err
	}

	// A reasonable size for the buffer to accommodate the largest possible
	// volume GUID path is 50 characters.
	volumeNameBuffer := make([]uint16, 50)
	if err := windows.GetVolumeNameForVolumeMountPoint(
		mountPointPointer, &volumeNameBuffer[0], 50); err != nil {
		return mountPoint, err
	}

	return syscall.UTF16ToString(volumeNameBuffer), nil
}

// newVssSnapshot creates a new vss snapshot. If creating the snapshots doesn't
// finish within the timeout an error is returned.
func newVssSnapshot(provider string,
	volume string, timeout time.Duration, filter volumeFilter, msgError ErrorHandler) (vssSnapshot, error) {
	if err := validateVssArchitecture(); err != nil {
		return vssSnapshot{}, err
	}

	deadline := time.Now().Add(timeout)

	oleIUnknown, err := initializeVssCOMInterface()
	if oleIUnknown != nil {
		defer oleIUnknown.Release()
	}
	if err != nil {
		return vssSnapshot{}, err
	}

	comInterface, err := queryInterface(oleIUnknown, UUID_IVSS)
	if err != nil {
		return vssSnapshot{}, err
	}

	/*
		https://microsoft.public.win32.programmer.kernel.narkive.com/aObDj2dD/volume-shadow-copy-backupcomplete-and-vss-e-bad-state

		CreateVSSBackupComponents();
		InitializeForBackup();
		SetBackupState();
		GatherWriterMetadata();
		StartSnapshotSet();
		AddToSnapshotSet();
		PrepareForBackup();
		DoSnapshotSet();
		GetSnapshotProperties();
		<Backup all files>
		VssFreeSnapshotProperties();
		BackupComplete();
	*/

	iVssBackupComponents := (*IVssBackupComponents)(unsafe.Pointer(comInterface))

	providerID, err := getProviderID(provider)
	if err != nil {
		iVssBackupComponents.Release()
		return vssSnapshot{}, err
	}

	if err := iVssBackupComponents.InitializeForBackup(); err != nil {
		iVssBackupComponents.Release()
		return vssSnapshot{}, err
	}

	if err := iVssBackupComponents.SetContext(VSS_CTX_BACKUP); err != nil {
		iVssBackupComponents.Release()
		return vssSnapshot{}, err
	}

	// see https://techcommunity.microsoft.com/t5/Storage-at-Microsoft/What-is-the-difference-between-VSS-Full-Backup-and-VSS-Copy/ba-p/423575

	if err := iVssBackupComponents.SetBackupState(false, false, VSS_BT_COPY, false); err != nil {
		iVssBackupComponents.Release()
		return vssSnapshot{}, err
	}

	err = callAsyncFunctionAndWait(iVssBackupComponents.GatherWriterMetadata,
		"GatherWriterMetadata", deadline)
	if err != nil {
		iVssBackupComponents.Release()
		return vssSnapshot{}, err
	}

	if isSupported, err := iVssBackupComponents.IsVolumeSupported(providerID, volume); err != nil {
		iVssBackupComponents.Release()
		return vssSnapshot{}, err
	} else if !isSupported {
		iVssBackupComponents.Release()
		return vssSnapshot{}, newVssTextError(fmt.Sprintf("Snapshots are not supported for volume "+
			"%s", volume))
	}

	const retryStartSnapshotSetSleep = 5 * time.Second
	var snapshotSetID ole.GUID
	for {
		var err error
		snapshotSetID, err = iVssBackupComponents.StartSnapshotSet()
		if errors.Is(err, VSS_E_SNAPSHOT_SET_IN_PROGRESS) && time.Now().Add(-retryStartSnapshotSetSleep).Before(deadline) {
			// retry snapshot set creation while deadline is not reached
			time.Sleep(retryStartSnapshotSetSleep)
			continue
		}

		if err != nil {
			iVssBackupComponents.Release()
			return vssSnapshot{}, err
		} else {
			break
		}
	}

	if err := iVssBackupComponents.AddToSnapshotSet(volume, providerID, &snapshotSetID); err != nil {
		iVssBackupComponents.Release()
		return vssSnapshot{}, err
	}

	mountPointInfo, err := addVssMountPoints(iVssBackupComponents, providerID, volume, filter)
	if err != nil {
		iVssBackupComponents.Release()
		return vssSnapshot{}, err
	}

	err = callAsyncFunctionAndWait(iVssBackupComponents.PrepareForBackup, "PrepareForBackup",
		deadline)
	if err != nil {
		// After calling PrepareForBackup one needs to call AbortBackup() before releasing the VSS
		// instance for proper cleanup.
		// It is not necessary to call BackupComplete before releasing the VSS instance afterwards.
		iVssBackupComponents.AbortBackup()
		iVssBackupComponents.Release()
		return vssSnapshot{}, err
	}

	err = callAsyncFunctionAndWait(iVssBackupComponents.DoSnapshotSet, "DoSnapshotSet",
		deadline)
	if err != nil {
		_ = iVssBackupComponents.AbortBackup() // Preserve the VSS preparation failure; abort is rollback only.
		iVssBackupComponents.Release()
		return vssSnapshot{}, err
	}

	var snapshotProperties vssSnapshotProperties
	err = iVssBackupComponents.GetSnapshotProperties(snapshotSetID, &snapshotProperties)
	if err != nil {
		_ = iVssBackupComponents.AbortBackup() // Preserve the VSS snapshot failure; abort is rollback only.
		iVssBackupComponents.Release()
		return vssSnapshot{}, err
	}

	loadMountPointSnapshotProperties(iVssBackupComponents, mountPointInfo, msgError)

	return vssSnapshot{
		iVssBackupComponents, snapshotSetID, snapshotProperties,
		snapshotProperties.GetSnapshotDeviceObject(), mountPointInfo, time.Until(deadline),
	}, nil
}

func loadMountPointSnapshotProperties(
	components *IVssBackupComponents,
	mountPoints map[string]mountPoint,
	reportError ErrorHandler,
) {
	for path, info := range mountPoints {
		if !info.isSnapshotted {
			continue
		}
		if err := components.GetSnapshotProperties(info.snapshotSetID, &info.snapshotProperties); err != nil {
			reportError(path, errors.Errorf(
				"VSS error: GetSnapshotProperties() for mount point %s returned error: ", path, err))
			info.isSnapshotted = false
		} else {
			info.snapshotDeviceObject = info.snapshotProperties.GetSnapshotDeviceObject()
		}
		mountPoints[path] = info
	}
}

func validateVssArchitecture() error {
	is64Bit, err := isRunningOn64BitWindows()
	if err != nil {
		return newVssTextError(fmt.Sprintf("Failed to detect windows architecture: %s", err.Error()))
	}
	if (is64Bit && runtime.GOARCH != "amd64") || (!is64Bit && runtime.GOARCH != "386") {
		return newVssTextError(fmt.Sprintf("executables compiled for %s can't use "+
			"VSS on other architectures. Please use an executable compiled for your platform.", runtime.GOARCH))
	}
	return nil
}

func addVssMountPoints(
	components *IVssBackupComponents,
	providerID *ole.GUID,
	volume string,
	filter volumeFilter,
) (map[string]mountPoint, error) {
	result := make(map[string]mountPoint)
	if filter == nil {
		return result, nil
	}
	mountPoints, err := enumerateMountedFolders(volume)
	if err != nil {
		return nil, newVssTextError(fmt.Sprintf("failed to enumerate mount points for volume %s: %s", volume, err))
	}
	for _, path := range mountPoints {
		result[path] = mountPoint{isSnapshotted: false}
		if !filter(path) {
			continue
		}
		isSupported, err := components.IsVolumeSupported(providerID, path)
		if err != nil || !isSupported {
			continue
		}
		var snapshotSetID ole.GUID
		if err := components.AddToSnapshotSet(path, providerID, &snapshotSetID); err != nil {
			return nil, err
		}
		result[path] = mountPoint{isSnapshotted: true, snapshotSetID: snapshotSetID}
	}
	return result, nil
}

// Delete deletes the created snapshot.
func (p *vssSnapshot) Delete() error {
	var err error
	if err = vssFreeSnapshotProperties(&p.snapshotProperties); err != nil {
		return err
	}

	for _, mountPoint := range p.mountPointInfo {
		if mountPoint.isSnapshotted {
			if err = vssFreeSnapshotProperties(&mountPoint.snapshotProperties); err != nil {
				return err
			}
		}
	}

	if p.iVssBackupComponents != nil {
		defer p.iVssBackupComponents.Release()

		deadline := time.Now().Add(p.timeout)

		err = callAsyncFunctionAndWait(p.iVssBackupComponents.BackupComplete, "BackupComplete",
			deadline)
		if err != nil {
			return err
		}

		if _, _, e := p.iVssBackupComponents.DeleteSnapshots(p.snapshotID); e != nil {
			err = newVssTextError(fmt.Sprintf("Failed to delete snapshot: %s", e.Error()))
			_ = p.iVssBackupComponents.AbortBackup() // Snapshot deletion is already complete; abort only releases VSS state.
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func getProviderID(provider string) (*ole.GUID, error) {
	providerLower := strings.ToLower(provider)
	switch providerLower {
	case "":
		return ole.IID_NULL, nil
	case "ms":
		return ole.NewGUID("{b5946137-7b9f-4925-af80-51abd60b20d5}"), nil
	}

	comInterface, err := ole.CreateInstance(CLSID_VSS_COORDINATOR, UIID_IVSS_ADMIN)
	if err != nil {
		return nil, err
	}
	defer comInterface.Release()

	vssAdmin := (*IVSSAdmin)(unsafe.Pointer(comInterface))

	enum, err := vssAdmin.QueryProviders()
	if err != nil {
		return nil, err
	}
	defer enum.Release()

	id := ole.NewGUID(provider)

	var props struct {
		objectType uint32
		provider   VssProviderProperties
	}
	for {
		count, err := enum.Next(1, unsafe.Pointer(&props))
		if err != nil {
			return nil, err
		}

		if count < 1 {
			return nil, errors.Errorf(`invalid VSS provider "%s"`, provider)
		}

		name := ole.UTF16PtrToString(props.provider.providerName)
		vssFreeProviderProperties(&props.provider)

		if id != nil && *id == props.provider.providerID ||
			id == nil && providerLower == strings.ToLower(name) {
			return &props.provider.providerID, nil
		}
	}
}

// asyncCallFunc is the callback type for callAsyncFunctionAndWait.
type asyncCallFunc func() (*IVSSAsync, error)

// callAsyncFunctionAndWait calls an async functions and waits for it to either
// finish or timeout.
func callAsyncFunctionAndWait(function asyncCallFunc, name string, deadline time.Time) error {
	iVssAsync, err := function()
	if err != nil {
		return err
	}

	if iVssAsync == nil {
		return newVssTextError(fmt.Sprintf("%s() returned nil", name))
	}

	timeout := time.Until(deadline)
	if timeout <= 0 {
		return newVssTextError(fmt.Sprintf("%s() deadline exceeded", name))
	}

	err = iVssAsync.WaitUntilAsyncFinished(timeout)
	iVssAsync.Release()
	return err
}

// loadIVssBackupComponentsConstructor finds the constructor of the VSS api
// inside the VSS dynamic library.
func loadIVssBackupComponentsConstructor() (*windows.LazyProc, error) {
	createInstanceName := "?CreateVssBackupComponents@@YAJPEAPEAVIVssBackupComponents@@@Z"

	if runtime.GOARCH == "386" {
		createInstanceName = "?CreateVssBackupComponents@@YGJPAPAVIVssBackupComponents@@@Z"
	}

	return findVssProc(createInstanceName)
}

// findVssProc find a function with the given name inside the VSS api
// dynamic library.
func findVssProc(procName string) (*windows.LazyProc, error) {
	vssDll := windows.NewLazySystemDLL("VssApi.dll")
	err := vssDll.Load()
	if err != nil {
		return &windows.LazyProc{}, err
	}

	proc := vssDll.NewProc(procName)
	err = proc.Find()
	if err != nil {
		return &windows.LazyProc{}, err
	}

	return proc, nil
}

// queryInterface is a wrapper around the windows QueryInterface api.
func queryInterface(oleIUnknown *ole.IUnknown, guid *ole.GUID) (*interface{}, error) {
	var ivss *interface{}

	result, _, _ := syscall.Syscall(oleIUnknown.VTable().QueryInterface, 3,
		uintptr(unsafe.Pointer(oleIUnknown)), uintptr(unsafe.Pointer(guid)),
		uintptr(unsafe.Pointer(&ivss)))
	if result != 0 {
		return nil, newVssError("QueryInterface failed", HRESULT(result))
	}

	return ivss, nil
}

// isRunningOn64BitWindows reports whether the process is running on 64-bit Windows.
func isRunningOn64BitWindows() (bool, error) {
	if runtime.GOARCH == "amd64" {
		return true, nil
	}

	isWow64 := false
	err := windows.IsWow64Process(windows.CurrentProcess(), &isWow64)
	if err != nil {
		return false, err
	}

	return isWow64, nil
}

// enumerateMountedFolders returns all mountpoints on the given volume.
func enumerateMountedFolders(volume string) ([]string, error) {
	var mountedFolders []string

	volumeNamePointer, err := syscall.UTF16PtrFromString(volume)
	if err != nil {
		return mountedFolders, err
	}

	volumeMountPointBuffer := make([]uint16, windows.MAX_LONG_PATH)
	handle, err := windows.FindFirstVolumeMountPoint(volumeNamePointer, &volumeMountPointBuffer[0],
		windows.MAX_LONG_PATH)
	if err != nil {
		// if there are no volumes an error is returned
		return mountedFolders, nil
	}

	// nolint:errcheck
	defer windows.FindVolumeMountPointClose(handle)

	volumeMountPoint := syscall.UTF16ToString(volumeMountPointBuffer)
	mountedFolders = append(mountedFolders, cleanupVolumeMountPoint(volume, volumeMountPoint))

	for {
		err = windows.FindNextVolumeMountPoint(handle, &volumeMountPointBuffer[0],
			windows.MAX_LONG_PATH)

		if err != nil {
			if err == syscall.ERROR_NO_MORE_FILES {
				break
			}

			return mountedFolders,
				newVssTextError("FindNextVolumeMountPoint() failed: " + err.Error())
		}

		volumeMountPoint := syscall.UTF16ToString(volumeMountPointBuffer)
		mountedFolders = append(mountedFolders, cleanupVolumeMountPoint(volume, volumeMountPoint))
	}

	return mountedFolders, nil
}

func cleanupVolumeMountPoint(volume, mountPoint string) string {
	return strings.ToLower(filepath.Join(volume, mountPoint) + string(filepath.Separator))
}
