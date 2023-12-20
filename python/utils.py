from threading import Semaphore

class Node:
    """
    Represents a node in a binary tree storing events.
    """

    def __init__(self, event, rightChild=None, leftChild=None):
        """
        Initializes a new Node instance.

        :param event: The event associated with the node.
        :param rightChild: The right child node.
        :param leftChild: The left child node.
        """
        self.event = event 
        self.leftChild = leftChild 
        self.rightChild = rightChild 

    def insert(self, new_event):
        """
        Inserts a new event into the binary tree.

        :param new_event: The event to be inserted.
        """
        # Adding a condition for when there is a remove operation
        # and a node with no event is left
        if self.event == None:
            self.event = new_event
            return 

        if new_event.timestamp < self.event.timestamp:
            if self.leftChild:
                self.leftChild.insert(new_event)
            else:
                self.leftChild = Node(new_event)
        else:
            if self.rightChild:
                self.rightChild.insert(new_event)
            else:
                self.rightChild = Node(new_event)

    def insert_node(self, new_node):
        """
        Inserts a new node into the binary tree.

        :param new_node: The node to be inserted.
        """
        # Adding condition for when there is a remove operation
        # and a node with no event is left
        if not self.event:
            self.event = new_node.event
            return 

        if new_node.event.timestamp < self.event.timestamp:
            if self.leftChild:
                self.leftChild.insert_node(new_node)
            else:
                self.leftChild = new_node
        else:
            if self.rightChild:
                self.rightChild.insert_node(new_node)
            else:
                self.rightChild = new_node


    def search(self, low, high, ret_val=None, value_to_ignore=None):
        """
        Searches for events within a specified timestamp range in the binary tree.

        :param low: The lower bound of the timestamp range (inclusive).
        :param high: The upper bound of the timestamp range (exclusive).
        :param ret_val: A list to store the matching events (default is an empty list).
        :return: A list of events within the specified timestamp range.
        """
        # Just asserting that lower value is smaller than higher value
        if low >= high:
            return []

        if ret_val is None:
            ret_val = []
            

        if self.event.timestamp < high and self.event.timestamp >= low:
            if self.leftChild:
                ret_val = self.leftChild.search(low, high, ret_val)
            ret_val.append(self.event)
            if self.rightChild:
                ret_val = self.rightChild.search(low, high, ret_val)
        elif  self.event.timestamp > high:
            if self.leftChild:
                self.leftChild.search(low, high, ret_val)
        elif self.event.timestamp < low:
            if self.rightChild:
                self.rightChild.search(low, high, ret_val)

        return ret_val

    def remove(self, value):
        if self.event.timestamp > value:
            if self.leftChild:
                self.leftChild.remove(value)
        elif self.event.timestamp < value:
            if self.rightChild:
                self.rightChild.remove(value)
        else:
            tmp_node = self.leftChild 
            if self.rightChild:
                self.event = self.rightChild.event
                self.rightChild = self.rightChild.rightChild 
                self.leftChild = self.rightChild.leftChild
                if self.leftChild:
                    self.leftChild.insert_node(tmp_node)
                else:
                    self.leftChild = tmp_node
            else:
                if self.leftChild:
                    self.event = self.leftChild.event
                    self.rightChild = self.leftChild.rightChild
                    self.leftChild = self.leftChild.leftChild
                else:
                    self.event = None # TODO: Think what to do in this condition

    def size(self):
        size = 1 
        if self.rightChild:
            size += self.rightChild.size()
        if self.leftChild:
            size += self.leftChild.size()

        return size

    def __str__(self):
        return f"Node with {self.event}, leftChild {self.leftChild} and rightChild {self.rightChild}"

class Event:
    """
    Represents an event with a specific event type and timestamp.
    """

    def __init__(self, event_type, timestamp):
        """
        Initializes a new Event instance.

        :param event_type: The type of the event.
        :param timestamp: The timestamp of the event.
        """
        self.event_type = event_type 
        self.timestamp = timestamp

    def __str__(self):
        return f'Event of type "{self.event_type}" with timestamp {self.timestamp}'


class EventStore:
    def __init__(self):
        self.event_map = {}
        self.semaphore = Semaphore()
 
    def insert(self, event_to_insert):

        """
        Inserts a new event into the event store.

        If the event type already exists in the store, the new event is inserted into the existing binary tree.
        If the event type is not present, a new binary tree is created for the event type.

        :param event_to_insert: The event to be inserted into the event store.
        """
        with self.semaphore:
            if self.event_map.get(event_to_insert.event_type):
                self.event_map.get(event_to_insert.event_type).insert(event_to_insert)
            else:
                self.event_map[event_to_insert.event_type] = Node(event_to_insert) 

    def remove_all(self, event_type):
        """
        Removes all events of a specific type from the event store.

        If the specified event type exists in the store, it is removed along with its associated binary tree.
        If the event type is not present, no action is taken.

        :param event_type: The type of events to be removed from the event store.
        """
        with self.semaphore:
            try:
                self.event_map.pop(event_type)
            except KeyError:
                pass

    def query(self, event_type, start_time, end_time):
        if start_time >= end_time:
            raise ValueError("end_time must be greater than start_time")

        events_tree = self.event_map.get(event_type)
        if events_tree:
            events = events_tree.search(start_time, end_time)
            return EventIterator(self.event_map.get(event_type), events)
        else:
            raise ValueError(f"No events of type {event_type}")

    def size(self):
        size = 0
        for event_type, event_store in self.event_map.items():
            size += event_store.size()

        return size


class EventIterator:
    """
    Represents an iterator over a collection of events.
    """
    def __init__(self, tree, events):
        self.tree = tree
        self.events = events
        self.has_moved = False
        self.index = -1

    def move_next(self):
        if not self.has_moved:
            self.has_moved = True

        self.index += 1

        try:
            self.events[self.index]
            return True
        except IndexError:
            return False

    def current(self):
        if not self.has_moved or self.index == -1:
            raise IndexError("The iteration did not start yet.")

        try:
            return self.events[self.index]
        except IndexError:
            raise StopIteration("Iterator has ended.")

    def remove(self):
        self.tree.remove(self.events[self.index].timestamp)
        self.events.pop(self.index) 
        self.index -= 1

    def __iter__(self):
        for idx, val in enumerate(self.events):
            yield val

    def to_list(self):
        return self.events

class CustomIterator:
    def __init__(self, tree, events):
        self.tree = tree
        self.events = events
        self.index = -1 

    def __iter__(self):
        for idx, val in enumerate(self.events):
            yield val

    def __next__(self):
        self.index += 1
        print(self.index)
        print(len(self.events))
        if self.index < len(self.events):
            return self.events[self.index]
        raise StopIteration

    def remove(self):
        self.tree.remove(self.events[self.index])
        self.events.pop(self.index) 
        # Note: If there is no more elements to pop,
        # it will throw an IndexError

    def to_list(self):
        return self.events

