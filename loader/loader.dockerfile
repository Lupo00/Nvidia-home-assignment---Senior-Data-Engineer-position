# Use an official Python runtime as a parent image
FROM python:3.8-slim-buster

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy relevant directories into the container at /usr/src/app
COPY ../common /usr/src/app/common
COPY loader/main.py /usr/src/app/
COPY loader/requirements.txt /usr/src/app/

# Install any needed packages specified in requirements.txt
RUN pip3 install -r requirements.txt

ENV PYTHONPATH="/usr/src/app/common:${PYTHONPATH}"

# Run app.py when the container launches
ENTRYPOINT ["python", "main.py"]
CMD ["--input_path", "input_files"]
