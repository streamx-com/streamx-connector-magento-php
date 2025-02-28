<?php declare(strict_types=1);

namespace StreamX\ConnectorCatalog\Model\ResourceModel;

use Magento\Framework\DB\Select;

interface SelectModifierInterface
{
    /**
     * Modifies the Select to select only rows that match additional conditions
     */
    public function modify(Select $select, int $storeId): void;

    /**
     * Modifies the Select to select only rows that match additional conditions that are the negation of conditions from the modify method
     */
    public function modifyNegate(Select $select, int $storeId): void;
}
