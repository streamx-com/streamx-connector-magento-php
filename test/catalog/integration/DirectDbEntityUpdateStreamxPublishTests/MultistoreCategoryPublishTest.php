<?php

namespace StreamX\ConnectorCatalog\test\integration\DirectDbEntityUpdateStreamxPublishTests;

use StreamX\ConnectorCatalog\Indexer\CategoryIndexer;
use StreamX\ConnectorCatalog\test\integration\utils\EntityIds;

/**
 * @inheritdoc
 */
class MultistoreCategoryPublishTest extends BaseDirectDbEntityUpdateTest {

    const INDEXER_IDS = [CategoryIndexer::INDEXER_ID];

    /** @test */
    public function shouldPublishActiveCategories() {
        // given: insert category as enabled for all stores by default, but disabled for store 1:
        $parentCategoryId = 2;

        $category = $this->insertCategory($parentCategoryId, 'Category name');
        self::$db->setCategoryNameAndStatus($category, 'Category name in first store', false, self::$store1Id);
        self::$db->setCategoryNameAndStatus($category, 'Category name in second store', true, self::$store2Id);

        // and
        $expectedKeyForStore1 = self::categoryKey($category, self::STORE_1_CODE);
        $expectedKeyForStore2 = self::categoryKey($category, self::STORE_2_CODE);
        $this->removeFromStreamX($expectedKeyForStore1, $expectedKeyForStore2);

        try {
            // when
            $this->reindexMview();

            // then
            $this->assertExactDataIsPublished($expectedKeyForStore2, 'added-category.json', [
                // provide values for placeholders in the validation file
                '"id":"' . $category->getEntityId() . '"' => '"id":"123456789"',
                'Category name in second store' => 'CATEGORY_NAME',
                "category-name-in-second-store-{$category->getEntityId()}" => 'CATEGORY_SLUG'
            ]);

            // and
            $this->assertDataIsNotPublished($expectedKeyForStore1);
        } finally {
            // and when
            $this->deleteCategory($category);
            $this->reindexMview();

            // then
            $this->assertDataIsUnpublished($expectedKeyForStore2);
        }
    }

    /** @test */
    public function shouldPublishCategoriesAssignedToStore() {
        // given: switch store with ID 2 to use a new category as its root category
        $rootCategoryIdForStore1 = 2; // this is the default root category for stores
        $rootCategoryForStore2 = $this->insertRootCategory('Root category for second store');
        $rootCategoryIdForStore2 = $rootCategoryForStore2->getEntityId();
        $this->changeRootCategoryForStore(self::$store2Id, $rootCategoryIdForStore2);

        // and: insert two new categories with different parent category IDs
        $store1Category = $this->insertCategory($rootCategoryIdForStore1, 'Bikes for first store');
        $store2Category = $this->insertCategory($rootCategoryIdForStore2, 'Bikes for second store');

        $store1CategoryId = $store1Category->getEntityId();
        $store2CategoryId = $store2Category->getEntityId();

        // and
        $expectedKeyForStore1 = self::categoryKey($store1Category, self::STORE_1_CODE);
        $expectedKeyForStore2 = self::categoryKey($store2Category, self::STORE_2_CODE);

        $unexpectedKeyForStore1 = self::categoryKey($store2Category, self::STORE_1_CODE);
        $unexpectedKeyForStore2 = self::categoryKey($store1Category, self::STORE_2_CODE);

        $this->removeFromStreamX($expectedKeyForStore1, $expectedKeyForStore2, $unexpectedKeyForStore1, $unexpectedKeyForStore2);

        try {
            // when
            $this->reindexMview();

            // then
            $this->assertExactDataIsPublished($expectedKeyForStore1, 'added-category.json', [
                // provide values for placeholders in the validation file
                '"id":"' . $store1CategoryId . '"' => '"id":"123456789"',
                'Bikes for first store' => 'CATEGORY_NAME',
                "bikes-for-first-store-$store1CategoryId" => 'CATEGORY_SLUG'
            ]);
            $this->assertDataIsNotPublished($unexpectedKeyForStore1);

            $this->assertExactDataIsPublished($expectedKeyForStore2, 'added-category.json', [
                // provide values for placeholders in the validation file
                '"id":"' . $store2CategoryId . '"' => '"id":"123456789"',
                'Bikes for second store' => 'CATEGORY_NAME',
                "bikes-for-second-store-$store2CategoryId" => 'CATEGORY_SLUG',
                // expect different parent category than default
                '"id":"' . $rootCategoryIdForStore2 . '",' => '"id":"2",',
                'Root category for second store' => 'Default Category',
                'root-category-for-second-store-' . $rootCategoryIdForStore2 => 'default-category-2'
            ]);
            $this->assertDataIsNotPublished($unexpectedKeyForStore2);
        } finally {
            // and when
            $this->deleteCategory($store1Category);
            $this->deleteCategory($store2Category);
            $this->deleteCategory($rootCategoryForStore2);
            try {
                $this->reindexMview();

                // then
                $this->assertDataIsUnpublished($expectedKeyForStore1);
                $this->assertDataIsUnpublished($expectedKeyForStore2);
            } finally {
                $this->changeRootCategoryForStore(self::$store2Id, $rootCategoryIdForStore1);
            }
        }
    }

    private function insertRootCategory(string $name): EntityIds {
        $rootCategoryId = 1;
        return self::$db->insertCategory($rootCategoryId, "$rootCategoryId", $name, true);
    }

    private function insertCategory(int $parentCategoryId, string $name): EntityIds {
        $rootCategoryId = 1;
        return self::$db->insertCategory($parentCategoryId, "$rootCategoryId/$parentCategoryId", $name, true);
    }

    private function changeRootCategoryForStore(int $storeId, int $categoryId): void {
        self::$db->execute("
            UPDATE store_group
               SET root_category_id = $categoryId
             WHERE default_store_id = $storeId"
        );
    }

    private function deleteCategory(EntityIds $categoryIds): void {
        CategoryAddAndDeleteTest::deleteCategory($categoryIds);
    }
}