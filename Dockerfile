FROM python:3.8-buster

# Change variables in ENTRYPOINT commands too
ENV USERNAME user
ENV APPDIR app
ENV HOMEDIR /home/${USERNAME}/

WORKDIR ${HOMEDIR}${APPDIR}

# install psycopg2 dependencies and other packages
RUN apt-get update && apt-get install -y python3-dev libpq-dev netcat locales nano

# Locale
RUN sed -i -e \
  's/# ru_RU.UTF-8 UTF-8/ru_RU.UTF-8 UTF-8/' /etc/locale.gen \
   && locale-gen

ENV LANG ru_RU.UTF-8
ENV LANGUAGE ru_RU:ru
ENV LC_LANG ru_RU.UTF-8
ENV LC_ALL ru_RU.UTF-8

# +Timezone
ENV TZ Asia/Yekaterinburg
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY ./requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["/bin/bash"]
