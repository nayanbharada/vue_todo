from rest_framework.viewsets import ModelViewSet
from todo.api.serializers import TodoSerializer
from todo.models import Todo


class TodoViewSet(ModelViewSet):
    serializer_class = TodoSerializer
    queryset = Todo.objects.all()
    pass