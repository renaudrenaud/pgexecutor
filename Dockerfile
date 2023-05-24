FROM python:3.11.0-slim-bullseye

LABEL maintainer="renaudrenaud"

COPY mycron /etc/cron.d/mycron

RUN apt-get update -yq \ 
&& apt-get install -y --no-install-recommends apt-utils \
&& apt install -yq git \
&& apt install -yq python3 \
&& apt install -yq python3-pip \
&& apt install -yq nano \
&& apt install -yq vim tmux watch\
&& apt install -yq cron

# RUN crontab - l { crontab -l; echo "*/10 * * * * python /home/pgexecutor/pgexecutor.py -c home/pgexecutor/select_version.yml>> /home/pgexecutor/pgexecutor.log 2>&1";}

WORKDIR /home
RUN git clone https://github.com/renaudrenaud/pgexecutor.git

WORKDIR /home/pgexecutor

#EXPOSE 8123

RUN python3 -m pip install -r requirements.txt
RUN update-rc.d cron defaults

RUN chmod 0644 /etc/cron.d/mycron
RUN crontab /etc/cron.d/mycron

# CMD ["python3","pgexecutor.py"]
# CMD ["python", "-m", "http.server", "8123"]
CMD ["cron", "-f"]


