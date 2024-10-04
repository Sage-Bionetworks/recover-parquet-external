# recover-parquet-external

[![Build and publish a Docker image](https://github.com/Sage-Bionetworks/recover-parquet-external/actions/workflows/docker-build.yml/badge.svg?branch=main)](https://github.com/Sage-Bionetworks/recover-parquet-external/actions/workflows/docker-build.yml)

## Purpose
This repository hosts code for the pipeline that syncs data from an internal processed data S3 bucket, transforms the data (filtering, de-identification, etc.), and then stores the transformed data in a separate S3 bucket as well as indexes the transformed data in Synapse.

## Requirements

-   R >= 4.0.0
-   Docker
-   Synapse account with relevant access permissions
-   Synapse authentication token

A Synapse authentication token is required for use of the Synapse APIs (e.g. the `synapser` package for R) and CLI client. For help with Synapse, Synapse APIs, Synapse authentication tokens, etc., please refer to the [Synapse documentation](https://help.synapse.org/docs/).

Your personal access token should have **View, Modify and Download** permissions; you can see your currently provisioned tokens [here](https://www.synapse.org/#!PersonalAccessTokens:). If you don't have a Synapse personal access token, refer to the instructiocs [here](https://sagebionetworks.jira.com/wiki/spaces/SC/pages/938836322/Service+Catalog+Provisioning#Create-a-Synapse-personal-access-token) to get a new token.

## Usage

There are two methods to run this pipeline:
1. [**Docker container**](#method-1-via-docker-container), or
2. [**Manually**](#method-2-manually)

### Set Synapse Personal Access Token

Regardless of which method you use, you need to set your Synapse Personal Access Token somewhere in your environment. See the examples below

1.  Option 1: For only the current shell session:

```Shell
export SYNAPSE_AUTH_TOKEN=<your-token>
```

2. Option 2: For all future shell sessions (modify your shell profile)

```Shell
# Open the profile file
nano ~/.bash_profile

# Append the following
SYNAPSE_AUTH_TOKEN=<your-token>
export SYNAPSE_AUTH_TOKEN

# Save the file
source ~/.bash_profile
```

### Method 1: via Docker Container

For the Docker method, there is a pre-published docker image available [here](https://github.com/Sage-Bionetworks/recover-parquet-external/pkgs/container/recover-parquet-external).

The primary purpose of using Docker is that the pre-made docker image in this repo contains instructions to:

- Create an environment with the dependencies needed by the pipeline
- Run a script containing the instructions for the pipeline, so that you don't need to manually find and run a specific script(s) or code

1.  Pull the docker image

```Shell
docker pull ghcr.io/sage-bionetworks/recover-parquet-external:main
```

2.  Run the docker container

```Shell
docker run \
  --name container-name \
  -e SYNAPSE_AUTH_TOKEN=$SYNAPSE_AUTH_TOKEN \
  ghcr.io/sage-bionetworks/recover-parquet-external:main
```

3. **(Optional)** Setup a scheduled job (AWS, cron, etc.) using the docker image to run the pipeline at a set frequency or when certain conditions are met

### Method 2: Manually

To run the pipeline manually, please follow the instructions in this section.

1. Clone this repo and set it as your working directory

```Shell
git clone https://github.com/Sage-Bionetworks/recover-parquet-external.git
```

2. Modify the parameters in the [config](config/config.yml) as needed
3. Run [install_requirements.R](install_requirements.R)

4. Run [internal_to_external_staging.R](scripts/main/internal_to_external_staging.R) to generate the external parquet datasets in the staging locations (S3 and Synapse).
5. Once the datasets in the staging location have been validated, run [staging_to_archive.R](scripts/main/staging_to_archive.R) to generate the validated external parquet datasets in the date-tagged prod Archive locations (S3 and Synapse). Currently, you must manually specify the name of the Synapse folder for the validated staging dataset version (e.g. 2024-10-01, 2024-09-10, etc.) you want to move from staging to Archive while running this script (e.g. `validated_date <- readline(...)`).
6. As needed, run [archive-to-current.R](scripts/main/archive-to-current.R) to update the Current Freeze version of the external parquet data in the appropriate locations (S3 and Synapse).
7. **(Optional)** Setup a scheduled job (AWS, cron, etc.) using the docker image to run the pipeline at a set frequency or when certain conditions are met
