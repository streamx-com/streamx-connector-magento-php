<?php declare(strict_types=1);

namespace StreamX\ConnectorCatalog\Model\ResourceModel\Product;

use Exception;
use Magento\Eav\Model\Entity\Attribute;
use Magento\Framework\App\ResourceConnection;
use Magento\Framework\DB\Adapter\AdapterInterface;
use Magento\Framework\DB\Select;
use Magento\Store\Model\Store;
use StreamX\ConnectorCatalog\Model\ProductMetaData;
use Zend_Db_Expr;

class ProductAttributesProvider
{
    private array $restrictedAttributes = [
        'quantity_and_stock_status',
        'options_container',
    ];

    private LoadAttributes $loadAttributes;
    private ResourceConnection $resourceConnection;
    /**
     * @var Attribute[]
     */
    private array $attributesById = [];
    private array $valuesByProductId = [];
    private ProductMetaData $productMetaData;

    public function __construct(
        LoadAttributes $loadAttributes,
        ResourceConnection $resourceConnection,
        ProductMetaData $productMetaData
    ) {
        $this->loadAttributes = $loadAttributes;
        $this->resourceConnection = $resourceConnection;
        $this->productMetaData = $productMetaData;
    }

    /**
     * @return array<int, array<string, array>> key = product id, value = array of: key = attribute code, value = one or more values of the attribute for that product
     * @throws Exception
     */
    public function loadAttributesData(int $storeId, array $productIds, array $requiredAttributeCodes): array
    {
        $this->attributesById = $this->loadAttributes->getAttributes();
        $tableAttributes = [];
        $attributeTypes = [];
        $selects = [];

        foreach ($this->attributesById as $attributeId => $attribute) {
            if ($this->canIndexAttribute($attribute, $requiredAttributeCodes)) {
                $tableAttributes[$attribute->getBackendTable()][] = $attributeId;

                if (!isset($attributeTypes[$attribute->getBackendTable()])) {
                    $attributeTypes[$attribute->getBackendTable()] = $attribute->getBackendType();
                }
            }
        }

        foreach ($tableAttributes as $table => $attributeIds) {
            $select = $this->getLoadAttributesSelect($storeId, $table, $attributeIds, $productIds);
            $selects[$table] = $select;
        }

        $this->valuesByProductId = [];

        if (!empty($selects)) {
            foreach ($selects as $select) {
                $values = $this->getConnection()->fetchAll($select);
                $this->processValues($values);
            }
        }

        return $this->valuesByProductId;
    }

    /**
     * @throws Exception
     */
    private function canIndexAttribute(Attribute $attribute, array $allowedAttributeCodes): bool
    {
        if ($attribute->isStatic()) {
            return false;
        }

        if (in_array($attribute->getAttributeCode(), $this->restrictedAttributes)) {
            return false;
        }

        if (empty($allowedAttributeCodes)) {
            return true;
        }

        return in_array($attribute->getAttributeCode(), $allowedAttributeCodes);
    }

    /**
     * @throws Exception
     */
    private function processValues(array $values): void
    {
        $productIdField = $this->productMetaData->getIdentifierField();

        foreach ($values as $value) {
            $attribute = $this->attributesById[$value['attribute_id']];
            $attributeValue = $value['value'];
            if ($attributeValue !== null) {
                $productId = $value[$productIdField];
                $attributeCode = $attribute->getAttributeCode();
                $attributeValues = $this->convertAttributeValueToArray($attribute, $attributeValue);
                $this->valuesByProductId[$productId][$attributeCode] = $attributeValues;
            }
        }
    }

    private function convertAttributeValueToArray(Attribute $attribute, $attributeValue): array {
        if ($attribute->getFrontendInput() === 'multiselect') {
            $options = explode(',', $attributeValue);
            if (!empty($options)) {
                $options = array_map([$this, 'parseValue'], $options);
            }
            return $options;
        }
        return [$attributeValue];
    }

    /**
     * Parse the option value - Cast to int if it's numeric
     * otherwise leave it as-is
     *
     * @param mixed $value
     *
     * @return mixed
     * @SuppressWarnings("unused")
     */
    private function parseValue($value)
    {
        return is_numeric($value) ? intval($value) : $value;
    }

    /**
     * Retrieve attributes load select
     *
     * @throws Exception
     */
    private function getLoadAttributesSelect(int $storeId, string $table, array $attributeIds, array $productIds): Select
    {
        //  Either row_id (enterprise/commerce version) or entity_id.
        $linkField = $this->productMetaData->getLinkField();
        $productIdField = $this->productMetaData->getIdentifierField();

        $joinStoreCondition = [
            "t_default.$linkField=t_store.$linkField",
            't_default.attribute_id=t_store.attribute_id',
            't_store.store_id=?',
        ];

        $joinCondition = $this->getConnection()->quoteInto(
            implode(' AND ', $joinStoreCondition),
            $storeId
        );

        $valueExpr = $this->getConnection()->getCheckSql(
            't_store.value_id IS NULL',
            't_default.value',
            't_store.value'
        );

        return $this->getConnection()->select()
            ->from(['entity' => $this->productMetaData->getEntityTable()], [$productIdField])
            ->joinInner(
                ['t_default' => $table],
                new Zend_Db_Expr("entity.$linkField = t_default.$linkField"),
                ['attribute_id']
            )
            ->joinLeft(
                ['t_store' => $table],
                $joinCondition,
                ['value' => $valueExpr]
            )
            ->where("entity.$productIdField IN (?)", $productIds)
            ->where('t_default.attribute_id IN (?)', $attributeIds)
            ->where(
                't_default.store_id = ?',
                $this->getConnection()->getIfNullSql('t_store.store_id', Store::DEFAULT_STORE_ID)
            );
    }

    private function getConnection(): AdapterInterface
    {
        return $this->resourceConnection->getConnection();
    }
}
