FROM rocker/r-ver

RUN apt-get update -y && apt-get upgrade -y && apt-get install -y python3 python3-pip python3-venv curl unzip git

RUN python3 -m pip install --upgrade pip
RUN pip install synapseclient

RUN git clone https://github.com/pranavanba/recover-parquet-external.git /root/recover-parquet-external
RUN Rscript /root/recover-parquet-external/install_requirements.R

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install

RUN curl "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/ubuntu_64bit/session-manager-plugin.deb" -o "session-manager-plugin.deb" \
    && dpkg -i session-manager-plugin.deb

RUN curl -o /root/synapse_creds.sh https://raw.githubusercontent.com/Sage-Bionetworks-IT/service-catalog-ssm-access/main/synapse_creds.sh \
    && chmod +x /root/synapse_creds.sh

RUN mkdir -p /root/.aws

RUN curl -o /root/.aws/config https://raw.githubusercontent.com/Sage-Bionetworks-IT/service-catalog-ssm-access/main/config

RUN sed -i -e "s|\"<PERSONAL_ACCESS_TOKEN>\"|\"\${AWS_SYNAPSE_TOKEN}\"\n|g" \
    -e "s|/absolute/path/to/synapse_creds.sh|/root/synapse_creds.sh|g" \
    /root/.aws/config

CMD R -e "q()" \
    && sed -i -e "s|\${AWS_SYNAPSE_TOKEN}|$AWS_SYNAPSE_TOKEN|g"\
    /root/.aws/config \
    && Rscript /root/recover-parquet-external/sts_synindex_external.R
