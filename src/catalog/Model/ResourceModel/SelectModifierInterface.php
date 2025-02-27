<?php declare(strict_types=1);

namespace StreamX\ConnectorCatalog\Model\ResourceModel;

use Magento\Framework\DB\Select;

interface SelectModifierInterface
{
    /**
     * Modifies the Select to select only rows that match additional conditions
     */
    public function modify(Select $select, int $storeId): void;
}
