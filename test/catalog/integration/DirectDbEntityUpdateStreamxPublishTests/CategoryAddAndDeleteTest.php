<?php

namespace StreamX\ConnectorCatalog\test\integration\DirectDbEntityUpdateStreamxPublishTests;

use StreamX\ConnectorCatalog\Indexer\CategoryIndexer;
use StreamX\ConnectorCatalog\test\integration\utils\EntityIds;

/**
 * @inheritdoc
 */
class CategoryAddAndDeleteTest extends BaseDirectDbEntityUpdateTest {

    const INDEXER_IDS = [CategoryIndexer::INDEXER_ID];

    /** @test */
    public function shouldPublishCategoryAddedDirectlyInDatabase_AndUnpublishDeletedCategory() {
        // given
        $categoryName = 'The new Category';

        // when
        $category = $this->insertNewCategory($categoryName);
        $expectedKey = self::categoryKey($category);

        try {
            // and
            $this->reindexMview();

            // then
            $this->assertExactDataIsPublished($expectedKey, 'added-category.json', [
                // provide values for placeholders in the validation file
                '"id":"' . $category->getEntityId() . '"' => '"id":"123456789"',
                'The new Category' => 'CATEGORY_NAME',
                "the-new-category-{$category->getEntityId()}" => 'CATEGORY_SLUG'
            ]);
        } finally {
            // and when
            $this->deleteCategory($category);
            $this->reindexMview();

            // then
            $this->assertDataIsUnpublished($expectedKey);
        }
    }

    private function insertNewCategory(string $categoryName): EntityIds {
        $rootCategoryId = 1;
        $parentCategoryId = 2;
        return self::$db->insertCategory($parentCategoryId, "$rootCategoryId/$parentCategoryId", $categoryName, true);
    }

    static function deleteCategory(EntityIds $categoryIds): void {
        self::$db->deleteById($categoryIds->getLinkFieldId(), [
            'catalog_category_entity_int' => self::$db->getEntityAttributeLinkField(),
            'catalog_category_entity_varchar' => self::$db->getEntityAttributeLinkField()
        ]);
        self::$db->deleteById($categoryIds->getEntityId(), [
            'catalog_category_entity' => 'entity_id'
        ]);
        if (self::$db->isEnterpriseMagento()) {
            self::$db->deleteById($categoryIds->getEntityId(), [
                'sequence_catalog_category' => 'sequence_value'
            ]);
        }
    }
}