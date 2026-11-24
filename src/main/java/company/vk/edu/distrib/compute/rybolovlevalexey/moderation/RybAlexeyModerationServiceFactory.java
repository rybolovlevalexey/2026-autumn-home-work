package company.vk.edu.distrib.compute.rybolovlevalexey.moderation;

import company.vk.edu.distrib.compute.moderation.ModerationDeadLetterTest;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactory;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactoryTest;

import java.io.IOException;

@ModerationServiceFactoryTest
@ModerationDeadLetterTest
public class RybAlexeyModerationServiceFactory implements ModerationServiceFactory {
    @Override
    public ModerationService create(String bootstrapServers) throws IOException {
        return new RybAlexeyModerationService(bootstrapServers);
    }
}
