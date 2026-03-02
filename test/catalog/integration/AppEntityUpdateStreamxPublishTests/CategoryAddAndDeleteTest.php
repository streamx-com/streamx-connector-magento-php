<?php

namespace StreamX\ConnectorCatalog\test\integration\AppEntityUpdateStreamxPublishTests;

use StreamX\ConnectorCatalog\Indexer\CategoryIndexer;
use StreamX\ConnectorCatalog\test\integration\utils\MagentoEndpointsCaller;

/**
 * @inheritdoc
 */
class CategoryAddAndDeleteTest extends BaseAppEntityUpdateTest {

    const INDEXER_IDS = [CategoryIndexer::INDEXER_ID];

    /** @test */
    public function shouldPublishCategoryAddedUsingMagentoApplication_AndUnpublishDeletedCategory() {
        // given
        $categoryName = 'The new Category';

        // when
        $parentCategoryId = 2;
        $categoryId = self::addCategory($categoryName, $parentCategoryId);

        // then
        $expectedKey = self::categoryKeyFromEntityId($categoryId);
        try {
            $this->assertExactDataIsPublished($expectedKey, 'added-category.json', [
                // provide values for placeholders in the validation file
                '"id":"' . $categoryId . '"' => '"id":"123456789"',
                'The new Category' => 'CATEGORY_NAME',
                "the-new-category-$categoryId" => 'CATEGORY_SLUG'
            ]);
        } finally {
            // and when
            self::deleteCategory($categoryId);

            // then
            $this->assertDataIsUnpublished($expectedKey);
        }
    }

    private function addCategory(string $categoryName, int $parentCategoryId): int {
        return (int) MagentoEndpointsCaller::call('category/add', [
            'categoryName' => $categoryName,
            'parentCategoryId' => $parentCategoryId
        ]);
    }

    private function deleteCategory(int $categoryId): void {
        MagentoEndpointsCaller::call('category/delete', [
            'categoryId' => $categoryId
        ]);
    }
}