# Use an official Python runtime as a parent image
FROM python:3.8-slim-buster

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy relevant directories into the container at /usr/src/app
COPY extractor/config /usr/src/app/config
COPY extractor/main.py /usr/src/app/
COPY extractor/source_files /usr/src/app/source_files
COPY extractor/requirements.txt /usr/src/app/
COPY common/ /usr/src/app/common

# Install any needed packages specified in requirements.txt
RUN pip3 install -r requirements.txt

ENV PYTHONPATH="/usr/src/app/common:${PYTHONPATH}"

# Run app.py when the container launches
ENTRYPOINT ["python", "main.py"]
CMD ["--batch_size", "10", "--input_path", "source_files", "--output_path", "output_files"]
