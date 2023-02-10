from prefect.deployments import Deployment
from etl_web_to_gcs import etl_web_to_gcs
from prefect.filesystems import GitHub 

storage = GitHub.load("ghs-webtogcs")

deployment = Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name="gh-deploy-webtogcs",
    storage=storage,
    output="q4_etl-web-to-gcs-deployment.yaml",
    entrypoint="Week2/workdir/flows/4/etl_web_to_gcs.py:etl_web_to_gcs"
   )

if __name__ == "__main__":
    deployment.apply()
