from django.db import models


# Create your models here.


class Todo(models.Model):
    todo_tile = models.CharField(max_length=20)
    todo_description = models.TextField()
    todo_creation = models.DateTimeField()

    def __str__(self):
        return self.todo_tile
