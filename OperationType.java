public enum OperationType {

    Begin("B"),
    Read("R"),
    Write("W"),
    MRead("M"),
    Commit("C"),
    Abort("A"),
    Delete("D");

    private final String opCode;

    private OperationType(final String opCode) {
        this.opCode = opCode;
    }

    public String showOpType() {
        return opCode;
    }

    public static OperationType decodeOperation(String line) throws Exception {

        char opCode = line.charAt(0);

        switch (opCode) {
            case 'B':
                return OperationType.Begin;
            case 'R':
                return OperationType.Read;
            case 'W':
                return OperationType.Write;
            case 'M':
                return OperationType.MRead;
            case 'C':
                return OperationType.Commit;
            case 'A':
                return OperationType.Abort;
            case 'D':
                return OperationType.Delete;
            default:
                throw new Exception("Unsupported Operation");
        }
    }
}
