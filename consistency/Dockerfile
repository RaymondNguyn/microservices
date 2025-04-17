FROM python:3

LABEL maintainer="rnguyen38@my.bcit.ca"
LABEL version="1.0"
LABEL description="This is the consistency service"

RUN mkdir /app

# Copys requirement.txt from the parent dir
COPY requirements.txt /app/requirements.txt

# Set working dir to /app in container
WORKDIR /app

RUN pip3 install -r requirements.txt

# Copy eerything from here to /app
COPY . /app

RUN groupadd -r appgroup && useradd -r -g appgroup -u 1444 appuser

# Create logs directory with proper permissions
RUN mkdir -p /logs && chown -R appuser:appgroup /logs && chmod 755 /logs
RUN mkdir -p /app/data && chown -R appuser:appgroup /app/data && chmod -R 755 /app/data

USER appuser

EXPOSE 9100

ENTRYPOINT [ "python3" ]

CMD [ "app.py" ]
