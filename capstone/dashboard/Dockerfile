FROM ubuntu:22.04

# Install Python and required libraries
RUN apt update && apt install -y python3-dev python3-pip libpq-dev

# Additional geospatial libraries
RUN apt install -y binutils libproj-dev gdal-bin

# Create the application directory
WORKDIR /app

# Copy the application files
COPY . .

# Install the required Python packages
RUN pip3 install -r requirements.txt

# Expose the application port
EXPOSE 8000

# Set command to run
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "dashboard.wsgi"]