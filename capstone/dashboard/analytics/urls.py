from rest_framework import routers
from analytics import viewsets

app_name = "analytics"
router = routers.DefaultRouter()
router.register(r"contests", viewsets.ContestViewSet, basename="contest")

urlpatterns = router.urls
