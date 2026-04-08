How to run:
run `make run` to run the project as docker containers.

The prebuilt images on Docker Hub are available only for linux/amd64, so if you are using other platform, you need to build the image by
yourself. You can run `make build` to build the image, and then run `make run` to run the containers.

The project is tested on a Windows host using Linux containers and on WSL, and should work on other Linux-based platforms (amd64/arm64).

To validate the result, check `csv/` post completion of `make run`
