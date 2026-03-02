<?php

namespace StreamX\ConnectorCatalog\test\integration\AppEntityUpdateStreamxPublishTests;

use StreamX\ConnectorCatalog\Indexer\CategoryIndexer;
use StreamX\ConnectorCatalog\Model\Config\Source\SlugOptionsSource;
use StreamX\ConnectorCatalog\test\integration\utils\ConfigurationEditUtils;
use StreamX\ConnectorCatalog\test\integration\utils\ConfigurationKeyPaths;
use StreamX\ConnectorCatalog\test\integration\utils\EntityIds;
use StreamX\ConnectorCatalog\test\integration\utils\MagentoEndpointsCaller;

/**
 * @inheritdoc
 */
class CategoryUpdateTest extends BaseAppEntityUpdateTest {

    const INDEXER_IDS = [CategoryIndexer::INDEXER_ID];

    /** @test */
    public function shouldPublishCategoryRenamedUsingMagentoApplication() {
        // given
        $defaultName = 'Gear';
        $changedName = 'Gear Articles';
        $categoryId = self::$db->getCategoryId($defaultName);

        // and
        $parentCategoryId = self::$db->getCategoryId('Default Category');
        $editedCategoryKey = self::categoryKey($categoryId);
        $parentCategoryKey = self::categoryKey($parentCategoryId);
        self::removeFromStreamX($editedCategoryKey, $parentCategoryKey);

        // when
        self::renameCategory($categoryId, $changedName);

        try {
            // then
            $this->assertExactDataIsPublished($editedCategoryKey, 'edited-gear-category.json');

            // and
            $this->assertTrue($this->isCurrentlyPublished($parentCategoryKey));
        } finally {
            self::renameCategory($categoryId, $defaultName);
            $this->assertExactDataIsPublished($editedCategoryKey, 'original-gear-category.json');
        }
    }

    /** @test */
    public function shouldPublishCategoryAndSubcategoriesWhenUrlKeyIsChangedUsingMagentoApplication() {
        // given
        $categoryName = 'Gear';
        $categoryId = self::$db->getCategoryId($categoryName);
        $defaultUrlKey = self::$db->getVarcharCategoryAttributeValue($categoryId, 'url_key');
        $changedUrlKey = 'super-gear';

        // and
        $parentCategoryId = self::$db->getCategoryId('Default Category');
        $subcategory1Id = self::$db->getCategoryId('Bags');
        $subcategory2Id = self::$db->getCategoryId('Fitness Equipment');
        $subcategory3Id = self::$db->getCategoryId('Watches');

        $editedCategoryKey = self::categoryKey($categoryId);
        $parentCategoryKey = self::categoryKey($parentCategoryId);
        $subcategory1Key = self::categoryKey($subcategory1Id);
        $subcategory2Key = self::categoryKey($subcategory2Id);
        $subcategory3Key = self::categoryKey($subcategory3Id);

        self::removeFromStreamX($editedCategoryKey, $parentCategoryKey, $subcategory1Key, $subcategory2Key, $subcategory3Key);

        // when
        ConfigurationEditUtils::setConfigurationValue(ConfigurationKeyPaths::SLUG_GENERATION_STRATEGY, SlugOptionsSource::URL_KEY_AND_ID);
        self::changeUrlKeyOfCategory($categoryId, $changedUrlKey);

        // and
        $originalSlug = 'gear-3';
        $expectedSlug = 'super-gear-3';

        try {
            // then
            $this->assertDataWithChangedSlugIsPublished($editedCategoryKey, 'original-gear-category.json', $originalSlug, $expectedSlug);

            // and: expect parent and subcategories to also be published, with the new slug of the edited category in their child or parent category data
            $this->assertDataWithChangedSlugIsPublished($parentCategoryKey, 'original-default-category-with-url-key-slugs.json', $originalSlug, $expectedSlug);
            $this->assertDataWithChangedSlugIsPublished($subcategory1Key, 'original-bags-category.json', $originalSlug, $expectedSlug);
            $this->assertDataWithChangedSlugIsPublished($subcategory2Key, 'original-fitness-category.json', $originalSlug, $expectedSlug);
            $this->assertDataWithChangedSlugIsPublished($subcategory3Key, 'original-watches-category.json', $originalSlug, $expectedSlug);
        } finally {
            self::changeUrlKeyOfCategory($categoryId, $defaultUrlKey);
            ConfigurationEditUtils::restoreConfigurationValue(ConfigurationKeyPaths::SLUG_GENERATION_STRATEGY);
            $this->assertExactDataIsPublished($editedCategoryKey, 'original-gear-category.json');
        }
    }

    private function assertDataWithChangedSlugIsPublished(string $key, string $validationFile, string $originalSlug, string $expectedSlug): void {
        $this->assertExactDataIsPublished($key, $validationFile, [
            '"slug":"' . $expectedSlug . '"' => '"slug":"' . $originalSlug . '"'
        ]);
    }

    /** @test */
    public function shouldPublishAllParentsOfEditedCategory_OnSeparatePublishKeys() {
        // given
        $categoryName = 'Tees';
        $changedName = 'The Tees';
        $categoryId = self::$db->getCategoryId($categoryName);

        // and
        $editedCategoryKey = self::categoryKey($categoryId);
        $expectedParentCategoryKey = self::categoryKey(self::$db->getCategoryId('Tops'));
        $expectedGrandParentCategoryKey = self::categoryKey(self::$db->getCategoryId('Men'));
        $expectedGrandGrandParentCategoryKey = self::categoryKey(self::$db->getCategoryId('Default Category'));
        $expectedGrandGrandGrandParentCategoryKey = self::categoryKey(self::$db->getCategoryId('Root Catalog'));

        self::removeFromStreamX(
            $editedCategoryKey,
            $expectedParentCategoryKey,
            $expectedGrandParentCategoryKey,
            $expectedGrandGrandParentCategoryKey,
            $expectedGrandGrandGrandParentCategoryKey
        );

        // when
        self::renameCategory($categoryId, $changedName);

        try {
            $this->assertExactDataIsPublished($editedCategoryKey, 'edited-tees-category.json');
            $this->assertExactDataIsPublished($expectedParentCategoryKey, 'original-tops-category.json');
            $this->assertExactDataIsPublished($expectedGrandParentCategoryKey, 'original-men-category.json');
            $this->assertExactDataIsPublished($expectedGrandGrandParentCategoryKey, 'original-default-category.json');
            $this->assertDataIsNotPublished($expectedGrandGrandGrandParentCategoryKey); // the root category is not published - by design
        } finally {
            self::renameCategory($categoryId, $categoryName);
            $this->assertExactDataIsPublished($editedCategoryKey, 'original-tees-category.json');
        }
    }

    private function renameCategory(EntityIds $categoryId, string $newName): void {
        MagentoEndpointsCaller::call('category/rename', [
            'categoryId' => $categoryId->getEntityId(),
            'newName' => $newName
        ]);
    }

    private function changeUrlKeyOfCategory(EntityIds $categoryId, string $newValue): void {
        MagentoEndpointsCaller::call('category/attribute/change', [
            'categoryId' => $categoryId->getEntityId(),
            'attributeCode' => 'url_key',
            'newValue' => $newValue
        ]);
    }
}