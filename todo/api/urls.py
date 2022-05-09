from django.urls import path, include
from rest_framework.routers import SimpleRouter
from .views import *
app_name = "todo"
router = SimpleRouter()
router.register("todo", TodoViewSet, basename="todo")

urlpatterns = [
    path("", include(router.urls))
]
