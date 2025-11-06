### 1. Install StreamX Connector in Magento
```bash
cd $MAGENTO_ROOT_DIR
bin/composer require streamx/magento-connector-php
```

Note: if the command asks you for a github access token - just press ENTER

### 2. Perform steps to enable the Connector's modules in Magento
```bash
cd $MAGENTO_ROOT_DIR
bin/magento module:enable StreamX_ConnectorCore
bin/magento module:enable StreamX_ConnectorCatalog
bin/magento cache:clean
bin/magento cache:flush
bin/magento setup:upgrade
bin/magento setup:di:compile
```

### 3. Start your StreamX instance (or verify it's already running)
```bash
streamx run -f path-to-your-mesh-file
```

You can find a basic mesh file at `src/catalog/test/resources/mesh.yaml`

### 4. Enable and configure StreamX Connector via the Magento Admin page
   - Click on `STORES` on the left panel
   - Click `Configuration` in the `Settings` area
   - Expand `STREAMX` section, click on `Connector` item, then expand `General Settings` section on the right
   - Select `Yes` for the `Enable StreamX Connector` setting and click the `Save Config` button
   - Switch Scope to website and select stores on the `List of stores to reindex`
   - Expand `StreamX Ingestion Settings` section below
   - Edit the settings you need, or leave the default values
   - Click the `Save Config` button
   - You can also provide separate values for `StreamX Ingestion Settings` for each store


### 5. Verify the Connector by indexing and publishing all categories to StreamX
```bash
cd $MAGENTO_ROOT_DIR
bin/magento indexer:reindex streamx_category_indexer
```

### 6. Verify if the data has been published to StreamX

If you used the `src/catalog/test/resources/mesh.yaml` to set up your local StreamX instance,
you can verify if the categories were successfully published to StreamX.

To do so, open your StreamX instance's Web Delivery Service endpoint in a web browser and verify the JSON output at a category URL.
Sample URL for Category with ID 6 is: http://localhost:8081/cat:6
Note: if you used a different Mesh Yaml file, the URL will be different.