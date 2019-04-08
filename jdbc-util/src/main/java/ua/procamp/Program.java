package ua.procamp;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Program {
    private Long id;
    private String name;
    private String description;
    private int version;
}
