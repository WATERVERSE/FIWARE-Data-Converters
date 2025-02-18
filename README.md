# Waterverse Data Converters Docker

## Setting up the docker

1. Build the docker
```bash
sudo docker build -t waterverse .
```

2. Run the docker

```bash
sudo docker run -d -p 8080:8080 waterverse
```

### To make sure the docker is running:

1. List running containers
```bash
sudo docker ps
```

2. Show container logs
```bash
sudo docker logs <container_id>
```
