#!/bin/bash
set -e # exit on 1st error

### PREREQUISITES:
# - Composer authentication to download Magento images is configured - see ../how-to-setup-local-development-environment.md

## Remove previous instance, if exists
if [ -d "magento" ]; then
    cd magento
    yes | bin/removeall || true # say yes to remove all containers; continue also if the magento instance wasn't yet installed
    cd ..
    rm -rf magento
fi

### Prepare directory for magento installation
mkdir magento
cd magento

### Download magento docker repository - see https://github.com/markshust/docker-magento
curl -s https://raw.githubusercontent.com/markshust/docker-magento/master/lib/template | bash
rm -rf .git

### Increase innodb-buffer-pool-size to avoid warnings in logs
sed -i '' 's|--max_allowed_packet|--innodb-buffer-pool-size=512M --max_allowed_packet|g' compose.yaml

### To avoid conflicts with StreamX, replace known ports that are used by both StreamX and Magento by default:
# - 80 and 443: port in both nginxs of StreamX and Magento
sed -i '' 's/80:8000/81:8000/g' compose.yaml
sed -i '' 's/443:8443/444:8443/g' compose.yaml
sed -i '' 's/$DOMAIN/$DOMAIN:444/g' bin/setup-install

# - 8080: ingestion port in StreamX and phpmyadmin port in Magento
sed -i '' 's/8080:80/8090:80/g' compose.dev.yaml

### Enable debugging
printf "\nXDEBUG_MODE=debug" >> env/phpfpm.env

### Disable warnings about weak admin password in Magento UI
sed -i '' 's/password123/P@SSw0rd123/g' env/magento.env

### Download source code and perform pre-installation.
# Depending on your repo.magento.com permissions provided in auth.json file, available versions for the below command are: community and enterprise
bin/download community 2.4.8

### Install the magento docker machinery
bin/setup magento.test

### Apply base urls into magento config database (by default both base urls are https://magento.test/)
bin/magento setup:store-config:set --base-url=https://magento.test:444/
bin/magento setup:store-config:set --base-url-secure=https://magento.test:444/

### Increase Admin IU session timeout from 15 minutes to 24 hours, to avoid frequent redirects to login page
bin/magento config:set admin/security/session_lifetime 86400

### Install sample data
bin/magento sampledata:deploy

### Install IDE autocomplete feature for xml configuration files such as mview.xml
bin/dev-urn-catalog-generate

### Upload StreamX Connector and Connector Test Tools to Magento
cd ..
bash scripts/copy-connector-to-magento.sh
bash scripts/copy-connector-test-endpoints-to-magento.sh

# Configure Magento to consider the uploaded connector source code as local repositories
cd magento
bin/composer config repositories.streamx-connector                 path  app/code/StreamX/Connector
bin/composer config repositories.streamx-connector-test-endpoints  path  app/code/StreamX/ConnectorTestEndpoints

# Install the connector (along with a module that turns off Two Factor Auth for development purposes and an extension for debugging or gathering code coverage)
# Note: if the command asks you for a github access token - just press ENTER
bin/composer require \
  "streamx/magento-connector-php" \
  "streamx/magento-connector-test-endpoints" \
  "markshust/magento2-module-disabletwofactorauth" \
  "ext-xdebug"

# Enable all modules
bin/magento module:enable --all
cd ..
bash scripts/reload-magento-modules.sh

# Additional steps
echo "Installation done"
echo "Please start your local StreamX instance (using test/resources/mesh.yaml as minimal mesh setup)"
read -rp "Press Enter when you're done..."

# Make StreamX Ingestion host visible from Magento containers
bash scripts/add-rest-ingestion-to-magento-network.sh

echo "Configuring the Connector and setting up stores..."
curl -X PUT https://magento.test:444/rest/all/V1/stores/setup
echo

# Start RabbitMQ consumer
echo "Starting RabbitMQ consumer. After that, the development environment is ready to use. You can run all tests to validate installation"
cd magento
bin/magento streamx:consumer:start
