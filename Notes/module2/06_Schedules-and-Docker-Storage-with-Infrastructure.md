Schedules & Docker Storage with Infrastructure

create Dockerfile
```
FROM prefecthq/prefect:2.7.7-python

COPY docker-requirements.txt .

RUN pip install -r docker-requirements --trusted-host ppi.python.org --no-cache-dir

COPY flows /opt/prefect/fows
COPY data /opt/prefect/data
```

run:
`docker image build -t dochamilton/prefect:zoom`

Create docker-requirements.txt
```
pandas==1.5.2
prefect-gcp[cloud_storage]==0.2.3
protobuf==4.21.11
pyarrow==10.0.1
```

Now make a docker block.
* Name it zoom or whatever.
* The pip packages are baked into the image.
* Next, specify the image: jrhamilton/prefect:zoom
* Select 'Image Pull Policy' to Always
* Click 'Auto Remove' to True
* Click 'Create'
* Copy the code to use

Side note: Can make a Docker block:
```
from prefect.infrastructure.docker import DockerContainer

docker_block = DockerContainer9
    image="dochamilton/prefect:zoom",
    image_pull_policy="ALWAYS",
    auto_renew=True,
)
docker_block.save("zoom", overwrite=True)
```

Make a deployment with python.
Create file: docker_deploy.py
Save to flows/03_deployment/

```python
from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
import paramertized_flow import etl_parent_flow

docker_block = DockerContainer.load("zoom")

docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name='docker-flow',
    infrastructure=docker_block
)

if __name__ == "__main__":
    docker_dep.apply()
```
Run:
`$ python flows/03_deploy/docker_deploy.py`
Click on Deployments in Orion to see deployment.

`prefect profile ls` shows
`$ prefect config set prefect_url="http://127.0.0.1:4200/api`
- So docker container can work with the prefect orion interface.

Fire up an agent:
`$ prefect agent start -q default`
to look for work in the default work queue

Spin up docker container
`$ prefect deployment etl-parent-flow/docker-flow -p months=[1,2]

So we just brought code to docker image to docker hub. Run that code on docker containers in local machine.
