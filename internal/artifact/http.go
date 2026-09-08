package artifact

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
)

type HTTPServer struct {
	service    *Service
	authorizer Authorizer
	echo       *echo.Echo
}

func NewHTTPServer(service *Service, authorizer Authorizer) (*HTTPServer, error) {
	if service == nil || authorizer == nil {
		return nil, ErrInvalid
	}
	for _, origin := range service.config.AllowedOrigins {
		if !ValidOrigin(origin) {
			return nil, ErrInvalid
		}
	}
	h := &HTTPServer{service: service, authorizer: authorizer, echo: echo.New()}
	h.echo.HideBanner = true
	h.echo.HidePort = true
	h.echo.HTTPErrorHandler = func(err error, c echo.Context) {
		if !c.Response().Committed {
			if responseErr := h.failure(c, err); responseErr != nil {
				slog.Warn("artifact response failed")
			}
		}
	}
	h.echo.Use(h.boundary)
	h.echo.GET("/healthz", func(c echo.Context) error { return c.NoContent(http.StatusNoContent) })
	h.echo.POST("/v1/uploads", h.reserve)
	h.echo.PUT("/v1/uploads/:artifact/parts", h.append)
	h.echo.POST("/v1/uploads/:artifact/finalize", h.finalize)
	h.echo.GET("/v1/artifacts/:artifact/manifests/:revision", h.manifest)
	h.echo.GET("/v1/artifacts/:artifact/manifests/:revision/objects/:object", h.object)
	return h, nil
}

func (h *HTTPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.echo.ServeHTTP(w, r) }

func (h *HTTPServer) boundary(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		c.Response().Header().Set("Cache-Control", "no-store")
		c.Response().Header().Set("X-Content-Type-Options", "nosniff")
		c.Response().Header().Set("Referrer-Policy", "no-referrer")
		c.Response().Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		origin := c.Request().Header.Get("Origin")
		if origin != "" {
			if !slices.Contains(h.service.config.AllowedOrigins, origin) {
				return ErrDenied
			}
			c.Response().Header().Set("Access-Control-Allow-Origin", origin)
			c.Response().Header().Set("Vary", "Origin")
			c.Response().Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
			c.Response().Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
			c.Response().Header().Set("Access-Control-Expose-Headers", "X-Artifact-SHA256")
			if c.Request().Method == http.MethodOptions {
				return c.NoContent(http.StatusNoContent)
			}
		}
		ctx, cancel := context.WithTimeout(c.Request().Context(), 30*time.Second)
		defer cancel()
		c.SetRequest(c.Request().WithContext(ctx))
		return next(c)
	}
}

func bearer(c echo.Context) (string, error) {
	headers := c.Request().Header.Values("Authorization")
	if len(headers) != 1 {
		return "", ErrDenied
	}
	scheme, token, ok := strings.Cut(headers[0], " ")
	if !ok || scheme != "Bearer" || token == "" || len(token) > 4096 {
		return "", ErrDenied
	}
	return token, nil
}

func (h *HTTPServer) reserve(c echo.Context) error {
	token, err := bearer(c)
	if err != nil {
		return err
	}
	var r Reservation
	if err := Decode(c.Request().Body, &r, MaxManifestBytes); err != nil {
		return err
	}
	if err := h.authorizer.Upload(c.Request().Context(), token, r); err != nil {
		return err
	}
	u, err := h.service.Reserve(c.Request().Context(), r)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, u)
}

func (h *HTTPServer) upload(c echo.Context) (Upload, error) {
	token, err := bearer(c)
	if err != nil {
		return Upload{}, err
	}
	u, err := h.service.loadUpload(c.Request().Context(), c.Param("artifact"))
	if err != nil {
		return Upload{}, ErrDenied
	}
	if err := h.authorizer.Upload(c.Request().Context(), token, u.Reservation); err != nil {
		return Upload{}, err
	}
	return u, nil
}

func (h *HTTPServer) append(c echo.Context) error {
	u, err := h.upload(c)
	if err != nil {
		return err
	}
	var part Part
	if err := Decode(c.Request().Body, &part, 2*objectLimit(u.Kind)+MaxManifestBytes); err != nil {
		return err
	}
	obj, err := h.service.Append(c.Request().Context(), u.ArtifactID, part)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, obj)
}

func (h *HTTPServer) finalize(c echo.Context) error {
	u, err := h.upload(c)
	if err != nil {
		return err
	}
	var request struct {
		State   string   `json:"state"`
		Capture *Capture `json:"capture,omitempty"`
	}
	if err := Decode(c.Request().Body, &request, MaxManifestBytes); err != nil {
		return err
	}
	ref, err := h.service.Finalize(c.Request().Context(), u.ArtifactID, request.State, request.Capture)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, ref)
}

func (h *HTTPServer) read(c echo.Context) (int64, error) {
	token, err := bearer(c)
	if err != nil {
		return 0, err
	}
	revision, err := Revision(c.Param("revision"))
	if err != nil {
		return 0, err
	}
	var projectID string
	if err := h.service.catalog.db.QueryRowContext(c.Request().Context(), "SELECT project_id FROM uploads WHERE id=?", c.Param("artifact")).Scan(&projectID); err != nil {
		return 0, ErrDenied
	}
	if err := h.authorizer.Read(c.Request().Context(), ReadAuthorization{ProjectID: projectID, Token: token, ArtifactID: c.Param("artifact"), Revision: revision}); err != nil {
		return 0, err
	}
	return revision, nil
}

func (h *HTTPServer) manifest(c echo.Context) error {
	revision, err := h.read(c)
	if err != nil {
		return err
	}
	body, ref, err := h.service.Manifest(c.Request().Context(), c.Param("artifact"), revision)
	if err != nil {
		return err
	}
	c.Response().Header().Set("X-Artifact-SHA256", ref.SHA256)
	return c.Blob(http.StatusOK, "application/json", body)
}

func (h *HTTPServer) object(c echo.Context) error {
	revision, err := h.read(c)
	if err != nil {
		return err
	}
	obj, body, err := h.service.ReadObject(c.Request().Context(), c.Param("artifact"), revision, c.Param("object"))
	if err != nil {
		return err
	}
	c.Response().Header().Set("X-Artifact-SHA256", obj.SHA256)
	c.Response().Header().Set("Content-Disposition", "attachment; filename=artifact")
	return c.Blob(http.StatusOK, obj.MediaType, body)
}

func (h *HTTPServer) failure(c echo.Context, err error) error {
	status, code := http.StatusServiceUnavailable, "storage_unreachable"
	for _, failure := range []struct {
		err    error
		status int
	}{{ErrDenied, 403}, {ErrInvalid, 422}, {ErrIntegrity, 422}, {ErrConflict, 409}, {ErrQuota, 413}, {ErrExpired, 410}, {ErrMissing, 404}, {ErrUnsupported, 501}, {ErrAuthorization, 503}} {
		if errors.Is(err, failure.err) {
			status, code = failure.status, failure.err.Error()
			break
		}
	}
	return c.JSON(status, struct {
		Code string `json:"code"`
	}{code})
}

func (h *HTTPServer) Serve(ctx context.Context, listener net.Listener, publisher Publisher) error {
	server := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 45 * time.Second, WriteTimeout: 45 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	finished := make(chan error, 1)
	go func() { finished <- server.Serve(listener) }()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case err := <-finished:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		case <-ticker.C:
			maintenanceCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			err := h.service.Maintain(maintenanceCtx)
			err = errors.Join(err, h.service.ReportHostedUsage(maintenanceCtx))
			if err == nil && publisher != nil {
				err = h.service.PublishPending(maintenanceCtx, publisher)
			}
			cancel()
			if err != nil && ctx.Err() == nil {
				slog.Warn("artifact maintenance or publication deferred")
				continue
			}
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			err := server.Shutdown(shutdownCtx)
			cancel()
			if err != nil {
				err = errors.Join(err, server.Close())
			}
			serveErr := <-finished
			if errors.Is(serveErr, http.ErrServerClosed) {
				serveErr = nil
			}
			return errors.Join(err, serveErr)
		}
	}
}
