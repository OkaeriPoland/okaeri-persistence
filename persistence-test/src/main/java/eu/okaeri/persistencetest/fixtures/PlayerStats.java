package eu.okaeri.persistencetest.fixtures;

import eu.okaeri.persistence.document.Document;
import eu.okaeri.validator.annotation.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class PlayerStats extends Document {

    @NotBlank
    @Size(min = 3, max = 16)
    private String username;

    @Min(1)
    @Max(100)
    private Integer level;

    @PositiveOrZero
    private Integer experience;

    @DecimalMin("0.0")
    @DecimalMax("100.0")
    private Double winRate;

    public PlayerStats(String username, Integer level) {
        this.username = username;
        this.level = level;
        this.experience = 0;
        this.winRate = 0.0;
    }
}
