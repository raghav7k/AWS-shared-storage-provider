package com.provider.sharedstorage.Controller;

import com.provider.sharedstorage.Service.ProviderService;
import org.springframework.web.bind.annotation.*;

@RequestMapping("api/v1")

@RestController
public class ProviderController {

    private final ProviderService providerService;

    public ProviderController(ProviderService providerService) {
        this.providerService = providerService;
    }

    @PostMapping("share")
    public Object createShare(@RequestBody String requestBody) {
        return providerService.createShare(requestBody);
    }

    @GetMapping("share")
    public Object getShares() {
        return providerService.getShares(null);
    }

    @GetMapping("share/{share-id}")
    public Object getShare(@PathVariable("share-id") String shareId) {
        return providerService.getShares(shareId);
    }

    @PatchMapping("share/{share-id}")
    public Object updateShare(@PathVariable("share-id") String shareId, @RequestBody String requestBody) {
        return providerService.updateShare(shareId, requestBody);
    }


    @PutMapping("share/{share-id}/datasets")
    public Object addDataset(@PathVariable("share-id") String shareId, @RequestBody String dataset) {
        return providerService.addDatasets(shareId, dataset);
    }

    @GetMapping("share/{share-id}/datasets")
    public Object getDatasets(@PathVariable("share-id") String shareId) {
        return providerService.getDatasets(shareId, null);
    }

    @PatchMapping("share/{share-id}/datasets/{dataset-id}")
    public Object updateDataset(@PathVariable("share-id") String shareId,
                                @PathVariable("dataset-id") String datasetId,
                                @RequestBody String requestBody) {
        return providerService.updateDataset(shareId, datasetId, requestBody);
    }

    @DeleteMapping("share/{share-id}/datasets/{dataset-id}")
    public Object deleteDataset(@PathVariable("share-id") String shareId, @PathVariable("dataset-id") String datasetId) {
        return providerService.deleteDataset(shareId, datasetId);
    }

    @PostMapping("share/{share-id}/subscribers")
    public Object addSubscriber(@PathVariable("share-id") String shareId, @RequestBody String subscriber) {
        return providerService.addSubscriber(shareId, subscriber);
    }

    @GetMapping("share/{share-id}/subscribers")
    public Object getSubscribers(@PathVariable("share-id") String shareId) {
        return providerService.getSubscribers(shareId);
    }

    @DeleteMapping("share/{share-id}/subscribers/{name}")
    public Object deleteSubscriber(@PathVariable("share-id") String shareId, @PathVariable("name") String subscriberName) {
        return providerService.deleteSubscriber(shareId, subscriberName);
    }


    @PostMapping("share/{share-id}/datasets/{dataset-id}/assets")
    public Object addAssetsToDataset(@PathVariable("share-id") String shareId,
                                     @PathVariable("dataset-id") String datasetId,
                                     @RequestBody String body) {
        return providerService.addAssetsToDataset(body, shareId, datasetId);
    }

    @GetMapping("share/{share-id}/datasets/{dataset-id}/assets")
    public Object getAssets(@PathVariable("share-id") String shareId,
                            @PathVariable("dataset-id") String datasetId) {
        return providerService.getAssets(shareId, datasetId);
    }

    @PutMapping("share/{share-id}/datasets/{dataset-id}/synchronization/run")
    public Object synAssets(@PathVariable("share-id") String shareId,
                                 @PathVariable("dataset-id") String datasetId) {
        return providerService.synAssumeRole(shareId, datasetId);
    }

}
