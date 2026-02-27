NETWORK_ID=$(docker network ls | grep "magento_default" | cut -d ' ' -f1)

# Allow calling ingestion endpoint from magento server
docker network connect "$NETWORK_ID" rest-ingestion.proxy

# Allow accessing magento images from blueprint resource-downloader service
docker network connect "$NETWORK_ID" resource-downloader.processor