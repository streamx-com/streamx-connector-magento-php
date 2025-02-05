<?php

declare(strict_types = 1);

namespace StreamX\ConnectorCatalog\Model\Attribute;

use Magento\Eav\Model\ResourceModel\Entity\Attribute\Option\Collection as OptionCollection;

class OptionCollectionToArray
{
    public static function execute(OptionCollection $collection, bool $loadSwatches): array
    {
        $res = [];

        foreach ($collection as $item) {
            $data = [];

            $data['value'] = (string)$item->getData('option_id');
            $data['label'] = $item->getData('value');
            $data['sort_order'] = (int)$item->getData('sort_order'); // TODO: can be removed

            if ($loadSwatches) {
                $data['swatch'] = [
                    'type' => (int)$item->getData('swatch_type'),
                    'value' => $item->getData('swatch_value')
                ];
            }

            $res[] = $data;
        }

        return $res;
    }
}
