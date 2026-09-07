package hubclient

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/digitaldrywood/detent/internal/tracker"
)

func changePath(item tracker.NativeWorkItemID, change, version string) (string, error) {
	path, err := nativeItemPath(item)
	if err != nil {
		return "", err
	}
	for _, part := range []struct{ value, prefix string }{{change, "change_"}, {version, "version_"}} {
		if part.value == "" {
			continue
		}
		if !strings.HasPrefix(part.value, part.prefix) || strings.ContainsAny(part.value, "/?#%\\") || part.value == part.prefix {
			return "", errors.New("invalid Change Request or version identity")
		}
	}
	path += "/changes"
	if change != "" {
		path += "/" + change
	}
	if version != "" {
		path += "/versions/" + version
	}
	return path, nil
}

func (c *NativeClient) Changes(ctx context.Context, item tracker.NativeWorkItemID) ([]tracker.ChangeRequest, error) {
	var result []tracker.ChangeRequest
	path, err := changePath(item, "", "")
	if err != nil {
		return nil, err
	}
	err = c.client.request(ctx, http.MethodGet, c.base()+path, nil, &result)
	return result, err
}

func (c *NativeClient) Change(ctx context.Context, item tracker.NativeWorkItemID, id string) (tracker.ChangeDetail, error) {
	var result tracker.ChangeDetail
	path, err := changePath(item, id, "")
	if err != nil || id == "" {
		return result, errors.New("valid Change Request identity is required")
	}
	err = c.client.request(ctx, http.MethodGet, c.base()+path, nil, &result)
	return result, err
}

func (c *NativeClient) CreateChange(ctx context.Context, item tracker.NativeWorkItemID, request tracker.CreateChange) (tracker.ChangeRequest, error) {
	var result tracker.ChangeRequest
	path, err := changePath(item, "", "")
	if err != nil {
		return result, err
	}
	request.Mutation = c.fencedMutation(ctx, item, request.Mutation)
	err = c.client.request(ctx, http.MethodPost, c.base()+path, request, &result)
	return result, err
}

func (c *NativeClient) PublishChangeVersion(ctx context.Context, item tracker.NativeWorkItemID, id string, request tracker.PublishChangeVersion) (tracker.ChangeVersion, error) {
	var result tracker.ChangeVersion
	path, err := changePath(item, id, "")
	if err != nil || id == "" {
		return result, errors.New("valid Change Request identity is required")
	}
	request.Mutation = c.fencedMutation(ctx, item, request.Mutation)
	err = c.client.request(ctx, http.MethodPost, c.base()+path+"/versions", request, &result)
	return result, err
}

func (c *NativeClient) ReviewChange(ctx context.Context, item tracker.NativeWorkItemID, id, version string, request tracker.ReviewChange) (tracker.ChangeReview, error) {
	var result tracker.ChangeReview
	path, err := changePath(item, id, version)
	if err != nil || id == "" || version == "" {
		return result, errors.New("valid Change Request and version identities are required")
	}
	err = c.client.request(ctx, http.MethodPost, c.base()+path+"/reviews", request, &result)
	return result, err
}

func (c *NativeClient) ChangeViewedFiles(ctx context.Context, item tracker.NativeWorkItemID, id, version string) ([]tracker.ChangeViewedFile, error) {
	var result []tracker.ChangeViewedFile
	path, err := changePath(item, id, version)
	if err != nil || id == "" || version == "" {
		return nil, errors.New("valid Change Request and version identities are required")
	}
	err = c.client.request(ctx, http.MethodGet, c.base()+path+"/viewed-files", nil, &result)
	return result, err
}

func (c *NativeClient) ViewChangeFile(ctx context.Context, item tracker.NativeWorkItemID, id, version string, request tracker.ViewChangeFile) (tracker.ChangeViewedFile, error) {
	var result tracker.ChangeViewedFile
	path, err := changePath(item, id, version)
	if err != nil || id == "" || version == "" {
		return result, errors.New("valid Change Request and version identities are required")
	}
	err = c.client.request(ctx, http.MethodPost, c.base()+path+"/viewed-files", request, &result)
	return result, err
}

func (c *NativeClient) SubmitChangeCheck(ctx context.Context, item tracker.NativeWorkItemID, id, version string, request tracker.SubmitChangeCheck) (tracker.ChangeCheck, error) {
	var result tracker.ChangeCheck
	path, err := changePath(item, id, version)
	if err != nil || id == "" || version == "" {
		return result, errors.New("valid Change Request and version identities are required")
	}
	err = c.client.request(ctx, http.MethodPost, c.base()+path+"/checks", request, &result)
	return result, err
}

func (c *NativeClient) DiscussChange(ctx context.Context, item tracker.NativeWorkItemID, id string, request tracker.DiscussChange) (tracker.ChangeDiscussion, error) {
	var result tracker.ChangeDiscussion
	path, err := changePath(item, id, "")
	if err != nil || id == "" {
		return result, errors.New("valid Change Request identity is required")
	}
	request.Mutation = c.fencedMutation(ctx, item, request.Mutation)
	err = c.client.request(ctx, http.MethodPost, c.base()+path+"/discussion", request, &result)
	return result, err
}

func (c *NativeClient) ApproveChangeReviewPolicy(ctx context.Context, request tracker.ApproveChangeReviewPolicy) (tracker.ChangeReviewPolicy, error) {
	var result tracker.ChangeReviewPolicy
	err := c.client.request(ctx, http.MethodPut, c.base()+"/change-review-policy", request, &result)
	return result, err
}

func (c *NativeConnector) FetchChanges(ctx context.Context, item tracker.NativeWorkItemID) ([]tracker.ChangeRequest, error) {
	return c.client.Changes(ctx, item)
}

func (c *NativeConnector) FetchChange(ctx context.Context, item tracker.NativeWorkItemID, id string) (tracker.ChangeDetail, error) {
	return c.client.Change(ctx, item, id)
}

func (c *NativeConnector) FetchNativeAttempts(ctx context.Context, item tracker.NativeWorkItemID) ([]tracker.NativeAttempt, error) {
	result := []tracker.NativeAttempt{}
	for cursor := ""; ; {
		page, err := c.client.Attempts(ctx, item, cursor)
		if err != nil {
			return nil, err
		}
		result = append(result, page.Items...)
		if page.NextCursor == "" {
			return result, nil
		}
		if page.NextCursor == cursor {
			return nil, errors.New("hub repeated attempt cursor")
		}
		cursor = page.NextCursor
	}
}
