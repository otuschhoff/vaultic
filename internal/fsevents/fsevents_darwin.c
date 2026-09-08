#include "fsevents_darwin.h"
#include <CoreFoundation/CoreFoundation.h>
#include <stdlib.h>

struct vaultic_fsevents_stream {
    FSEventStreamRef stream;
};

static void vaultic_callback(
    ConstFSEventStreamRef stream,
    void *context,
    size_t count,
    void *paths,
    const FSEventStreamEventFlags flags[],
    const FSEventStreamEventId ids[]) {
    (void)stream;
    char **path_array = (char **)paths;
    uintptr_t handle = (uintptr_t)context;
    for (size_t index = 0; index < count; index++) {
        vaulticFSEventsCallback(handle, path_array[index], ids[index], flags[index]);
    }
}

uint64_t vaultic_fsevents_current_id(uint64_t device) {
    (void)device;
    return FSEventsGetCurrentEventId();
}

int vaultic_fsevents_journal_uuid(uint64_t device, char *buffer, size_t length) {
    CFUUIDRef uuid = FSEventsCopyUUIDForDevice((dev_t)device);
    if (uuid == NULL) {
        return 0;
    }
    CFStringRef value = CFUUIDCreateString(kCFAllocatorDefault, uuid);
    CFRelease(uuid);
    if (value == NULL) {
        return 0;
    }
    Boolean copied = CFStringGetCString(value, buffer, (CFIndex)length, kCFStringEncodingUTF8);
    CFRelease(value);
    return copied ? 1 : 0;
}

vaultic_fsevents_stream *vaultic_fsevents_create(
    uint64_t device,
    char **paths,
    size_t path_count,
    uint64_t since,
    uintptr_t handle) {
    CFMutableArrayRef watched = CFArrayCreateMutable(kCFAllocatorDefault, (CFIndex)path_count, &kCFTypeArrayCallBacks);
    if (watched == NULL) {
        return NULL;
    }
    for (size_t index = 0; index < path_count; index++) {
        CFStringRef path = CFStringCreateWithCString(kCFAllocatorDefault, paths[index], kCFStringEncodingUTF8);
        if (path == NULL) {
            CFRelease(watched);
            return NULL;
        }
        CFArrayAppendValue(watched, path);
        CFRelease(path);
    }

    FSEventStreamContext context = {0, (void *)handle, NULL, NULL, NULL};
    FSEventStreamCreateFlags create_flags =
        kFSEventStreamCreateFlagFileEvents |
        kFSEventStreamCreateFlagNoDefer |
        kFSEventStreamCreateFlagWatchRoot;
    FSEventStreamRef stream = FSEventStreamCreateRelativeToDevice(
        kCFAllocatorDefault, vaultic_callback, &context, (dev_t)device,
        watched, (FSEventStreamEventId)since, 0.0, create_flags);
    CFRelease(watched);
    if (stream == NULL) {
        return NULL;
    }
    FSEventStreamScheduleWithRunLoop(stream, CFRunLoopGetCurrent(), kCFRunLoopDefaultMode);
    if (!FSEventStreamStart(stream)) {
        FSEventStreamInvalidate(stream);
        FSEventStreamRelease(stream);
        return NULL;
    }
    vaultic_fsevents_stream *result = calloc(1, sizeof(*result));
    if (result == NULL) {
        FSEventStreamStop(stream);
        FSEventStreamInvalidate(stream);
        FSEventStreamRelease(stream);
        return NULL;
    }
    result->stream = stream;
    return result;
}

void vaultic_fsevents_poll(double seconds) {
    CFRunLoopRunInMode(kCFRunLoopDefaultMode, seconds, true);
}

void vaultic_fsevents_destroy(vaultic_fsevents_stream *stream) {
    if (stream == NULL) {
        return;
    }
    FSEventStreamStop(stream->stream);
    FSEventStreamInvalidate(stream->stream);
    FSEventStreamRelease(stream->stream);
    free(stream);
}

uint32_t vaultic_fsevents_flag_must_scan(void) { return kFSEventStreamEventFlagMustScanSubDirs; }
uint32_t vaultic_fsevents_flag_user_dropped(void) { return kFSEventStreamEventFlagUserDropped; }
uint32_t vaultic_fsevents_flag_kernel_dropped(void) { return kFSEventStreamEventFlagKernelDropped; }
uint32_t vaultic_fsevents_flag_ids_wrapped(void) { return kFSEventStreamEventFlagEventIdsWrapped; }
uint32_t vaultic_fsevents_flag_history_done(void) { return kFSEventStreamEventFlagHistoryDone; }
uint32_t vaultic_fsevents_flag_root_changed(void) { return kFSEventStreamEventFlagRootChanged; }
uint32_t vaultic_fsevents_flag_mount(void) { return kFSEventStreamEventFlagMount; }
uint32_t vaultic_fsevents_flag_unmount(void) { return kFSEventStreamEventFlagUnmount; }