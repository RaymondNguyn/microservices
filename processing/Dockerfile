FROM python:3

LABEL maintainer:"rnguyen38@my.bcit.ca"

RUN mkdir /app

# Copys requirement.txt from the parent dir
COPY ../requirements.txt /app/requirements.txt

# Set working dir to /app in container
WORKDIR /app

RUN pip3 install -requirements.txt

# Copy eerything from here to /app
COPY . /app

RUN chown -R nobody:nogroup /app
USER nobody

EXPOSE 8100

ENTRYPOINT [ "python3" ]

CMD [ "app.py" ]
