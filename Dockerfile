# syntax=docker/dockerfile:1
   
FROM julia:latest
WORKDIR /DoingRightNow
COPY . .
EXPOSE 3000

RUN apt update
RUN apt install -y git



CMD ["julia", "--project=@."]