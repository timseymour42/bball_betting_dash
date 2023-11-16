FROM public.ecr.aws/lambda/python:3.11

# Create a working directory
WORKDIR /var/task

# Copy your requirements file into the image
COPY requirements.txt .

RUN yum install -y zip

# Install dependencies
RUN pip install -r requirements.txt --target /var/task.

# Copy the rest of your code
COPY . .
