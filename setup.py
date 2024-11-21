from setuptools import find_packages, setup

setup(
   name='pipeline_flespi',
   version='1.0',
   install_requires=[
        # "google-cloud-tasks==2.2.0",
        # "google-cloud-pubsub>=0.1.0",
        # "google-cloud-storage==1.39.0",
        # "google-cloud-bigquery==2.6.2",
        # "google-cloud-secret-manager==2.0.0",
        "apache-beam[gcp]",
   ],
   packages=find_packages()
)