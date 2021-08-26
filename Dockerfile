FROM continuumio/miniconda3:latest

RUN git clone https://github.com/ICESat2-SlideRule/sliderule.git; git clone https://github.com/aws/aws-sdk-cpp.git; 

RUN apt update; apt install -y build-essential libreadline-dev liblua5.3-dev cmake libcurl4-openssl-dev libssl-dev uuid-dev zlib1g-dev unzip wget

RUN cd aws-sdk-cpp; git checkout version1.8; mkdir aws_sdk_build; cd aws_sdk_build; cmake .. -DCMAKE_BUILD_TYPE=Release -DBUILD_ONLY="s3;transfer" -DBUILD_SHARED_LIBS=OFF -DENABLE_TESTING=OFF; make; make install;

RUN wget  https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip
RUN /aws/install

RUN pip install pybind11 obspy numpy jupyter xarray h5py pyasdf pooch

RUN cd /sliderule; make config; make -j 2; make install
RUN cd /sliderule; cmake -DCMAKE_BUILD_TYPE=Release -DPYTHON_BINDINGS=ON -DUSE_H5_PACKAGE=ON -DUSE_AWS_PACKAGE=ON -DUSE_LEGACY_PACKAGE=ON -DUSE_CCSDS_PACKAGE=ON -Dpybind11_DIR=/opt/conda/lib/python3.9/site-packages/pybind11/share/cmake/pybind11 /sliderule; make -j 2;

RUN cp /sliderule/*.so /
