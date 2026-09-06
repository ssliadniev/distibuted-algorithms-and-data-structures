class CommitTimeoutError(TimeoutError):
    """
    Raised when a leader cannot commit a client command before the deadline.

    Attributes:
        index: The log index of the command that failed to commit.
        term: The leader's term when the command was appended.
    """

    def __init__(self, index: int, term: int) -> None:
        self.index = index
        self.term = term

        super().__init__(f"Command at index {index} was not committed in term {term}.")


class NodeStoppedError(RuntimeError):
    """
    Raised when a client attempts an operation while the node runtime is halted.
    """
