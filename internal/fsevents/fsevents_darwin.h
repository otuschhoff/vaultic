#include <CoreServices/CoreServices.h>
#include <stdint.h>

typedef struct vaultic_fsevents_stream vaultic_fsevents_stream;

extern void vaulticFSEventsCallback(uintptr_t handle, char *path, uint64_t event_id, uint32_t flags);

uint64_t vaultic_fsevents_current_id(uint64_t device);
int vaultic_fsevents_journal_uuid(uint64_t device, char *buffer, size_t length);
vaultic_fsevents_stream *vaultic_fsevents_create(
    uint64_t device,
    char **paths,
    size_t path_count,
    uint64_t since,
    uintptr_t handle);
void vaultic_fsevents_poll(double seconds);
void vaultic_fsevents_destroy(vaultic_fsevents_stream *stream);

uint32_t vaultic_fsevents_flag_must_scan(void);
uint32_t vaultic_fsevents_flag_user_dropped(void);
uint32_t vaultic_fsevents_flag_kernel_dropped(void);
uint32_t vaultic_fsevents_flag_ids_wrapped(void);
uint32_t vaultic_fsevents_flag_history_done(void);
uint32_t vaultic_fsevents_flag_root_changed(void);
uint32_t vaultic_fsevents_flag_mount(void);
uint32_t vaultic_fsevents_flag_unmount(void);