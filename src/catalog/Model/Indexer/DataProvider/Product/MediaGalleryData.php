<?php declare(strict_types=1);

namespace StreamX\ConnectorCatalog\Model\Indexer\DataProvider\Product;

use StreamX\ConnectorCatalog\Model\ProductMetaData;
use StreamX\ConnectorCatalog\Model\ResourceModel\Product\Gallery as Resource;
use StreamX\ConnectorCore\Api\DataProviderInterface;
use StreamX\ConnectorCore\Indexer\ImageUrlManager;

class MediaGalleryData implements DataProviderInterface
{
    private Resource $resourceModel;
    private ProductMetaData $productMetaData;
    private ImageUrlManager $imageUrlManager;
    private array $rowIdToEntityId = [];

    public function __construct(
        Resource $resource,
        ProductMetaData $productMetaData,
        ImageUrlManager $imageUrlManager
    ) {
        $this->resourceModel = $resource;
        $this->productMetaData = $productMetaData;
        $this->imageUrlManager = $imageUrlManager;
    }

    /**
     * @inheritdoc
     */
    public function addData(array &$indexData, int $storeId): void
    {
        $this->mapRowIdToEntityId($indexData);
        $linkField = $this->productMetaData->getLinkField();
        $linkFieldIds = array_column($indexData, $linkField);

        $gallerySet = $this->resourceModel->loadGallerySet($linkFieldIds, $storeId);

        foreach ($indexData as &$productData) {
            $productData['gallery'] = [];
        }

        foreach ($gallerySet as $mediaImage) {
            $linkFieldId  = $mediaImage['row_id'];
            $entityId = $this->rowIdToEntityId[$linkFieldId] ?? $linkFieldId;

            $imageUrl = $this->imageUrlManager->getProductImageUrl($mediaImage['file']);
            $imageAlt = $this->getLabel($mediaImage);
            $image = [
                'url' => $imageUrl,
                'alt' => $imageAlt
            ];

            $indexData[$entityId]['gallery'][] = $image;

            if (isset($indexData[$entityId]['primaryImage'])) {
                if ($indexData[$entityId]['primaryImage']['url'] === $imageUrl) {
                    $indexData[$entityId]['primaryImage']['alt'] = $imageAlt;
                }
            }
        }

        $this->rowIdToEntityId = [];
    }

    private function mapRowIdToEntityId(array $products): void
    {
        $linkField = $this->productMetaData->getLinkField();
        $identifierField = $this->productMetaData->getIdentifierField();

        if ($identifierField !== $linkField) {
            foreach ($products as $entityId => $product) {
                $this->rowIdToEntityId[$product[$linkField]] = $entityId;
            }
        }
    }

    private function getLabel(array $image): string
    {
        if (isset($image['label'])) {
            return $image['label'];
        }

        if (isset($image['label_default'])) {
            return $image['label_default'];
        }

        return '';
    }
}
