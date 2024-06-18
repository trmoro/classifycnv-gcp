FROM python:3.8-slim

# Copy data
COPY . ./

# Install Python3 requirements packages
RUN pip install -r requirements.txt

#Permission on chainfiles
RUN chmod -R 755 ./chainfiles

# Install dependencies
RUN apt update
RUN apt install -y bedtools

#Set Entrypoint for GCP
ENTRYPOINT ["python"]
CMD ["app.py"]
