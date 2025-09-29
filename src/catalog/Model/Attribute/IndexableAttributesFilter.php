<?php

namespace StreamX\ConnectorCatalog\Model\Attribute;

use StreamX\ConnectorCatalog\Model\Attributes\AttributeDefinition;
use StreamX\ConnectorCatalog\Model\Attributes\ChildProductAttributes;
use StreamX\ConnectorCatalog\Model\Attributes\ProductAttributes;

class IndexableAttributesFilter {

    private ProductAttributes $productAttributes;
    private ChildProductAttributes $childProductAttributes;

    public function __construct(
        ProductAttributes $productAttributes,
        ChildProductAttributes $childProductAttributes
    ) {
        $this->productAttributes = $productAttributes;
        $this->childProductAttributes = $childProductAttributes;
    }

    /**
     * Filters the given attributes to return only those of the attributes that are
     * currently selected as indexable for main (non-child) products in the Connector settings.
     *
     * @param AttributeDefinition[] $attributes
     * @return int[] attribute IDs
     */
    public function filterIdsOfIndexableProductAttributes(array $attributes, int $storeId): array {
        return $this->filterIdsOfIndexableAttributes(
            $attributes,
            $this->productAttributes->getAttributesToIndex($storeId)
        );
    }

    /**
     * Filters the given attributes to return only those of the attributes that are
     * currently selected as indexable for child (variant) products in the Connector settings.
     *
     * @param AttributeDefinition[] $attributes
     * @return int[] attribute IDs
     */
    public function filterIdsOfIndexableChildProductAttributes(array $attributes, int $storeId): array {
        return $this->filterIdsOfIndexableAttributes(
            $attributes,
            $this->childProductAttributes->getAttributesToIndex($storeId)
        );
    }

    /**
     * @param AttributeDefinition[] $attributes
     * @param string[] $attributeCodesToIndex
     * @return int[] attribute IDs
     */
    private function filterIdsOfIndexableAttributes(array $attributes, array $attributeCodesToIndex): array {
        // empty list of attributes to index always means: index all attributes.
        if (empty($attributeCodesToIndex)) {
            return array_map(fn($attribute) => $attribute->getId(), $attributes);
        }

        $result = [];
        foreach ($attributes as $attribute) {
            if (in_array($attribute->getCode(), $attributeCodesToIndex)) {
                $result[] = $attribute->getId();
            }
        }
        return $result;
    }

    public function isIndexableProductAttribute(string $attributeCode, int $storeId): bool {
        return in_array($attributeCode, $this->productAttributes->getAttributesToIndex($storeId));
    }

    public function isIndexableChildProductAttribute(string $attributeCode, int $storeId): bool {
        return in_array($attributeCode, $this->childProductAttributes->getAttributesToIndex($storeId));
    }
}