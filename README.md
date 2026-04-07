How to run:
run `make run` to run the project as docker containers.

The build on docker hub is only for windows/amd64, so if you are using other platform, you need to build the image by
yourself. You can run `make build` to build the image, and then run `make run` to run the containers.

The project is tested on windows/amd64, and it should work on other platforms as well.

To validate the result, check `csv/` post completion of `make run`
