FROM gordonwatts/rucio-base:v1.0.1

WORKDIR /usr/func-adl-rucio

# Get python3 up and running.
RUN yum -y install python36 python36-pip wget

# And everything we need to run this guy
COPY requirements.txt .
RUN pip3 install -r requirements.txt 

# Get the runner in
COPY tools/*.py ./

# And running it:
ENTRYPOINT ["python3", "func-adl-rucio-mq.py"]
