# CloudPyASDF

This is a python module that aims at improving the efficiency reading ASDF data from cloud environment.

## PyASDF
[PyASDF](https://github.com/SeismicData/pyasdf) is the python ASDF API that is already well developed. You could read the documents [here](https://seismicdata.github.io/pyasdf/index.html) to learn how to use it. We want to make our cloud API similar to the local API so that the advantage of cloud environment could be maximized.

## H5coro
See [sliderule project](https://github.com/ICESat2-SlideRule/sliderule) to learn more about H5coro.


## Docker
Docker provides a "clean" environment that minimize the computation environment issue when users with different machines could test the module in an almost identical way.

Here we provides a `Dockerfile` that can be used to generate the same Ubuntu intance that was used for our developing and testing. Get the Docker, and run build in the directory that contains the `Dockerfile`.
```shell
$ docker build .
```
