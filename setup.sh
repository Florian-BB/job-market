docker build -t fastapi:latest . -f dockerfiles/Dockerfastapi
docker build -t dash:latest . -f dockerfiles/Dockerdash
docker-compose up