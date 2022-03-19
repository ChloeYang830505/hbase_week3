package dto;

import lombok.Builder;
import lombok.Data;

/**
 * @author YangChunping
 * @version 1.0
 * @date 2022/3/16 16:04
 * @description
 */
@Data
@Builder
public class StudentBasicInfo {
    String name;
    private String studentId;
    private String studentClass;
    private String understanding;
    private String programming;
}
