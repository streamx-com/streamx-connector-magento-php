<?php

declare(strict_types = 1);

namespace StreamX\ConnectorCatalog\Model\Attribute;

use StreamX\ConnectorCatalog\Model\ResourceModel\Product\ProductAttributesProvider;

use Magento\Eav\Model\ResourceModel\Entity\Attribute\Option\Collection as OptionCollection;
use Magento\Eav\Model\ResourceModel\Entity\Attribute\Option\CollectionFactory;
use Magento\Eav\Model\Entity\Attribute;
use Magento\Eav\Model\Entity\Attribute\Source\AbstractSource;
use Magento\Eav\Model\Entity\Attribute\Source\Table as SourceTable;
use Magento\Swatches\Model\Swatch;

class LoadOptions
{
    private ProductAttributesProvider $attributeDataProvider;
    private CollectionFactory $collectionFactory;
    private array $optionsByAttribute = [];

    public function __construct(
        CollectionFactory $collectionFactory,
        ProductAttributesProvider $attributeDataProvider
    ) {
        $this->collectionFactory = $collectionFactory;
        $this->attributeDataProvider = $attributeDataProvider;
    }

    public function execute(string $attributeCode, int $storeId): array
    {
        $attributeModel = $this->attributeDataProvider->getAttributeByCode($attributeCode);
        $attributeModel->setStoreId($storeId);

        return $this->loadOptions($attributeModel);
    }

    private function loadOptions(Attribute $attribute): array
    {
        $key = $attribute->getId() . '_' . $attribute->getStoreId();

        if (!isset($this->optionsByAttribute[$key])) {
            if ($this->useSourceModel($attribute)) {
                $source = $attribute->getSource();
                $options = $source->getAllOptions();
            } else {
                $loadSwatches = $this->isVisualSwatch($attribute);
                $optionCollection = $this->getOptionCollection($attribute, $loadSwatches);
                $options = OptionCollectionToArray::execute($optionCollection, $loadSwatches);
            }

            $this->optionsByAttribute[$key] = $options;
        }

        return $this->optionsByAttribute[$key];
    }

    private function useSourceModel(Attribute $attribute): bool
    {
        $source = $attribute->getSource();

        if ($source instanceof AbstractSource && !($source instanceof SourceTable)) {
            return true;
        }

        return false;
    }

    private function getOptionCollection(Attribute $attribute, bool $loadSwatches): OptionCollection
    {
        $attributeId = $attribute->getAttributeId();
        $storeId = $attribute->getStoreId();

        /** @var OptionCollection $options */
        $options = $this->collectionFactory->create();
        $options->setOrder('sort_order', 'asc');
        $options->setAttributeFilter($attributeId)
            ->setStoreFilter($storeId);

        if ($loadSwatches) {
            $options->getSelect()->joinLeft(
                ['swatch_table' => $options->getTable('eav_attribute_option_swatch')],
                'swatch_table.option_id = main_table.option_id AND swatch_table.store_id = 0',
                [
                    'swatch_value' => 'value',
                    'swatch_type' => 'type',
                ]
            );
        }

        return $options;
    }

    // TODO add test for exporting product attributes that use swatches
    private function isVisualSwatch(Attribute $attribute): bool
    {
        return $attribute->getData('swatch_input_type') === Swatch::SWATCH_INPUT_TYPE_VISUAL
            || $attribute->getData('swatch_input_type') === Swatch::SWATCH_INPUT_TYPE_TEXT;
    }

}
