This instruction allows debugging StreamX Connector code deployed to a Magento server.
It's designed to work with PHP Storm IDE.

1. `PhpStorm` menu -> Settings -> Debug -> Enable Listening for PHP Debug Connections: click `Start listening` -> click `OK`
2. `Run` menu -> Edit Configurations -> New configuration (plus sign):
   - configuration type: `PHP Remote Debug`
   - give it some name of your choice
   - under Configuration enable the `Filter debug connection by IDE key`
   - add new server: host = `magento-app-1`, port = `444`, debugger = `Xdebug`
   - check `Use path mappings checkbox`
   - add mapping from `[your full local path to streamx-connector-magento]/src` to `/var/www/html/app/code/StreamX/Connector/src`
   - add mapping from `[your full local path to streamx-connector-magento]/connector-test-endpoints` to `/var/www/html/app/code/StreamX/ConnectorTestEndpoints`
   - click OK
   - then, for the `IDE key (session id)` enter: `PHPSTORM`
   - then open the server settings again, to map also `[your full local path to streamx-connector-magento]/magento/src` to `/var/www/html`.
     - if no such path is available to be selected in the window - as a workaround add the mapping manually:
     - open `.idea/workspace.xml` file in a text editor (the file may be hidden)
     - below the existing mapping: `<mapping local-root="$PROJECT_DIR$/src" remote-root="/var/www/html/app/code/StreamX/Connector/src" />`
     - add mapping: `<mapping local-root="$PROJECT_DIR$/magento/src" remote-root="/var/www/html" />`

3. Prepare your browser to become a tool to trigger Connector PHP code to stop at breakpoints:
   - install https://addons.mozilla.org/en-US/firefox/addon/xdebug-helper-for-firefox and configure it to use `PHPSTORM` as the IDE Key. Enable it.
   - go to (for example) admin product edit page
   - make sure to enable the Debug plugin on the page
   - in PHP Storm, start the PHP Remote Debug configuration that you've created
   - put breakpoints in Connector code that handles gathering or preparing products / publishing them to StreamX
   - edit a product (for example its price) as admin in the browser
   - observe code stopping at your breakpoints

4. If you call Magento REST endpoints to trigger actions in your tests
   (using the `MagentoEndpointsCaller::call` method) - breakpoints will be triggered automatically without the need to use a Web Browser.