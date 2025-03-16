FROM python:3

LABEL maintainer="rnguyen38@my.bcit.ca"
LABEL version="1.0"
LABEL description="This is the analzyer service"

RUN mkdir /app

# Copys requirement.txt from the parent dir
COPY requirements.txt /app/requirements.txt

# Set working dir to /app in container
WORKDIR /app

RUN pip3 install -r requirements.txt

# Copy eerything from here to /app
COPY . /app

RUN chown -R nobody:nogroup /app
USER nobody

EXPOSE 8900

ENTRYPOINT [ "python3" ]

CMD [ "app.py" ]
