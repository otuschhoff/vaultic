# Docker image

## Build

From the root of this repository run:

```
./docker/build.sh
```

image name will be `otuschhoff/vaultic:latest`

## Run

Set environment variable `VAULTIC_REPOSITORY` and map volume to directories and
files like:

```
docker run --rm --hostname my-host -ti \
    -v $HOME/.vaultic/passfile:/pass \
    -v $HOME/importantdirectory:/data \
    -e VAULTIC_REPOSITORY=rest:https://user:pass@hostname/ \
    otuschhoff/vaultic -p /pass backup /data
```

Vaultic relies on the hostname for various operations. Make sure to set a static
hostname using `--hostname` when creating a Docker container, otherwise Docker
will assign a random hostname each time.
