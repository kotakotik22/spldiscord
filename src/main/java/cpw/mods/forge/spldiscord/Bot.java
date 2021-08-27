package cpw.mods.forge.spldiscord;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import discord4j.common.util.Snowflake;
import discord4j.core.DiscordClient;
import discord4j.core.DiscordClientBuilder;
import discord4j.core.GatewayDiscordClient;
import discord4j.core.event.domain.lifecycle.ReadyEvent;
import discord4j.core.event.domain.message.ReactionAddEvent;
import discord4j.core.object.entity.Attachment;
import discord4j.core.object.entity.Member;
import discord4j.core.object.entity.Message;
import discord4j.core.object.entity.User;
import discord4j.core.object.entity.channel.TextChannel;
import discord4j.core.object.reaction.ReactionEmoji;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufMono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;
import reactor.util.function.Tuple3;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Bot {
    public static class Config implements IConfig {
        public Config(String requestChannel, String updateChannel, String guild, String approverRole, String modDir, String token, String caPath, String caKey) {
            this.requestChannel = requestChannel;
            this.updateChannel = updateChannel;
            this.guild = guild;
            this.approverRole = approverRole;
            this.modDir = modDir;
            this.token = token;
            this.caPath = caPath;
            this.caKey = caKey;
        }

        public Config(Snowflake requestChannel, Snowflake updateChannel, Snowflake guild, Snowflake approverRole, String modDir, String token, String caPath, String caKey) {
            this(requestChannel.asString(), updateChannel.asString(), guild.asString(), approverRole.asString(), modDir, token, caPath, caKey);
        }

        protected String updateChannel = "?", guild = "?", approverRole = "?", requestChannel = "?", modDir = "?", token = "?", caPath = "?", caKey = "?";

        @Override
        public String getCaKey() {
            return caKey;
        }

        @Override
        public String getCaPath() {
            return caPath;
        }

        protected static Snowflake s(String str) {
            return Snowflake.of(str);
        }

        @Override
        public Snowflake getRequestChannel() {
            return s(requestChannel);
        }

        @Override
        public Snowflake getUpdateChannel() {
            return s(updateChannel);
        }

        @Override
        public Snowflake getGuild() {
            return s(guild);
        }

        @Override
        public Snowflake getApproverRole() {
            return s(approverRole);
        }

        @Override
        public Path getModDir() {
            return Paths.get(modDir);
        }

        @Override
        public String getToken() {
            return token;
        }

        public String toJson() {
            return new GsonBuilder().setPrettyPrinting().create().toJson(this);
        }
    }

    public interface IConfig {
        Snowflake getRequestChannel();
        Snowflake getUpdateChannel();
        Snowflake getGuild();
        Snowflake getApproverRole();
        Path getModDir();
        String getToken();
        String getCaPath();
        String getCaKey();

        default List<Snowflake> getMonitoredChannels() {
            return Arrays.asList(getRequestChannel(), getUpdateChannel());
        }

        static Config fromEnv() {
            return new Config(
                    Util.env("REQUEST_CHANNEL"),
                    Util.env("MODS_CHANNEL"),
                    Util.env("GUILD"),
                    Util.env("APPROVER_ROLE"),
                    Util.defaultEnv("OUTPUT_DIR", "."),
                    Util.defaultEnv("BOT_TOKEN", "0"),
                    Util.defaultEnv("CA_PATH", "/app/volume/cacert.pem"),
                    Util.defaultEnv("CA_KEY","/app/volume/ca.key")
            );
        }

        static Config fromJson(String json) {
            return new Gson().fromJson(json, Config.class);
        }

        static Config fromJsonFile(String path) {
            File file = new File(path);
            Config c = null;
            if(file.exists()) {
                try {
                    c = fromJson(FileUtils.readFileToString(file, (String) null));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                c = fromEnv();
            }
            try {
                FileUtils.writeStringToFile(file, c.toJson(), (String) null);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return c;
        }

        static Config fromJsonFile() throws IOException {
            File f = new File("splconfigpath.txt");
            if(!f.exists()) {
                FileUtils.writeStringToFile(f, "spl.json", (String) null);
            }
            return fromJsonFile(FileUtils.readFileToString(f, (String) null));
        }
    }

//    private static final Snowflake REQUESTCHANNEL = Util.env("REQUEST_CHANNEL");
//    private static final Snowflake MODUPDATECHANNEL = Util.env("MODS_CHANNEL");
//    private static final Snowflake GUILD = Util.env("GUILD");
//    private static final Snowflake APPROVERROLE = Util.env("APPROVER_ROLE");
    private static final ReactionEmoji.Unicode THUMBSUP = ReactionEmoji.unicode("\ud83d\udc4d");
    private static final ReactionEmoji.Unicode UNAMUSED = ReactionEmoji.unicode("\ud83d\ude12");
    private static final ReactionEmoji.Unicode TICK = ReactionEmoji.unicode("\u2714");
    private static final ReactionEmoji.Unicode CROSS = ReactionEmoji.unicode("\u274C");
//    private static final Path MODDIR = Paths.get(Util.defaultEnv("OUTPUT_DIR", "."));
    private final Map<Snowflake, Function<Flux<ReactionAddEvent>, Publisher<Void>>> messageHandlerByChannel;
    private Snowflake me;

    protected IConfig config;

    public static void main(String[] args) throws IOException {
        LogManager.getLogger().info("HELLO");
        new Bot(IConfig.fromJsonFile());
    }

    public Bot(IConfig config) {
        this.config = config;

        messageHandlerByChannel = new HashMap<>();
        messageHandlerByChannel.put(config.getRequestChannel(), this::handleAuthChannel);
        messageHandlerByChannel.put(config.getUpdateChannel(), this::handleModChannel);

        DiscordClient client = DiscordClientBuilder.create(config.getToken()).build();
        GatewayDiscordClient eventDispatcher = client.login().block();

        eventDispatcher.on(ReadyEvent.class).subscribe((e) -> {
            me = e.getSelf().getId();
        });
        Mono<Void> catchup = eventDispatcher.on(ReadyEvent.class).flatMap(this::catchupMessages).then();
        Mono<Void> reaction = eventDispatcher.on(ReactionAddEvent.class)
                .transform(this::filterReactionEvents)
                .groupBy(ReactionAddEvent::getChannelId)
                .transform(this::dispatchReactionEventToHandler)
                .then();
        Runtime.getRuntime().addShutdownHook(new Thread(()->eventDispatcher.onDisconnect().block()));
        Mono.when(catchup, reaction).doOnError(this::error).block();
    }

    private Mono<Void> catchupMessages(final ReadyEvent evt) {
        Snowflake now = Snowflake.of(Instant.now());
        return evt.getClient()
                .getGuildById(config.getGuild())
                .flatMap(g -> g.getChannelById(config.getRequestChannel()).ofType(TextChannel.class))
                .flatMapMany(tc -> tc.getMessagesBefore(now))
                .transform(this::authChannelFilter)
                .flatMap(this::handleValidAuthMessage)
                .then();
    }

    private Flux<ReactionAddEvent> filterReactionEvents(final Flux<ReactionAddEvent> reactions) {
        return reactions
                .filter(event -> Objects.equals(THUMBSUP, event.getEmoji()))
                .filter(event -> config.getMonitoredChannels().contains(event.getChannelId()));
    }

    private Flux<Void> dispatchReactionEventToHandler(Flux<GroupedFlux<Snowflake, ReactionAddEvent>> flux) {
        return flux.flatMap(gf -> gf.transform(messageHandlerByChannel.get(gf.key())));
    }

    private void error(final Throwable throwable) {
        LogManager.getLogger().throwing(throwable);
    }

    private Publisher<Void> handleAuthChannel(final Flux<ReactionAddEvent> reactionEvent) {
        return reactionEvent
                .flatMap(ReactionAddEvent::getMessage)
                .transform(this::authChannelFilter)
                .flatMap(this::handleValidAuthMessage)
                .then();
    }

    private Publisher<Void> handleModChannel(final Flux<ReactionAddEvent> reactionEvent) {
        return reactionEvent
                .flatMap(ReactionAddEvent::getMessage)
                .transform(this::modChannelFilter)
                .flatMap(this::handleValidModMessage)
                .then();
    }

    private Flux<Message> authChannelFilter(Flux<Message> messages) {
        return messages
                .filter(mess -> !mess.getAttachments().isEmpty())
                .filter(mess -> mess.getAttachments().stream().anyMatch(attachment -> attachment.getFilename().endsWith(".csr")))
                .transform(this::filterMessageReactions);
    }

    private Flux<Message> modChannelFilter(Flux<Message> messages) {
        return messages.transform(this::filterMessageReactions);
    }

    private Flux<Message> filterMessageReactions(Flux<Message> messages) {
        return messages
                .filterWhen(m -> m
                        .getReactors(THUMBSUP)
                        .flatMap(r -> r.asMember(config.getGuild()))
                        .any(mem -> mem.getRoleIds().contains(config.getApproverRole())))
                .filterWhen(m -> m
                        .getReactors(TICK)
                        .map(User::getId)
                        .collect(Collectors.toSet())
                        .map(set -> !set.contains(me)))
                .filterWhen(m -> m
                        .getReactors(CROSS)
                        .map(User::getId)
                        .collect(Collectors.toSet())
                        .map(set -> !set.contains(me)))
                .filterWhen(m -> m
                        .getReactors(UNAMUSED)
                        .map(User::getId)
                        .collect(Collectors.toSet())
                        .map(set -> !set.contains(me)));
    }

    private Mono<Void> handleValidAuthMessage(Message msg) {
        Mono<InputStream> cert = msg.getAttachments()
                .stream()
                .findFirst()
                .map(attachment -> downloadAttachment(attachment, CertSigner::signCSR)).orElse(Mono.empty());
        Mono<Member> userName = msg.getAuthorAsMember();
        Mono<TextChannel> channel = msg.getChannel().ofType(TextChannel.class);
        Mono<Message> message = Mono.zip(cert, userName, channel).flatMap(this::buildCertificateAttachment);
        Mono<Void> ok = msg.addReaction(TICK);
        return message.then(ok).onErrorResume(t->this.reactionError(t, msg));
    }

    private Mono<Void> handleValidModMessage(final Message message) {
        Mono<Void> ok = message.addReaction(TICK);
        Mono<Void> files;
        Optional<String> content = Optional.of(message.getContent()); // yeah i know i can just use the string but this makes porting easier
        if (message.getAttachments().stream().anyMatch(att->att.getFilename().endsWith(".jar"))) {
            files = Flux.from(message
                    .getAttachments()
                    .stream()
                    .findFirst()
                    .map(attachment -> downloadAttachment(attachment,
                            inputStream -> saveFile(inputStream, attachment.getFilename())))
                    .orElse(Mono.empty()))
                    .then(ok);
        } else if (message.getEmbeds().stream().anyMatch(e->e.getUrl().map(s->s.endsWith(".jar")).orElse(Boolean.FALSE))) {
            files = Flux
                    .fromIterable(message.getEmbeds())
                    .flatMap(e->e.getUrl()
                            .map(url-> downloadUrl(url,
                                    inputStream -> saveFile(inputStream, getFilenameFromUrlString(url))))
                            .orElse(Mono.empty()))
                    .then(ok);
        } else if (content.map(s->s.startsWith("http") && s.endsWith(".jar")).orElse(Boolean.FALSE)) {
            LogManager.getLogger().info("Downloading from message URL: {}", content.get());
            files = Flux.from(content
                    .map(url -> downloadUrl(url,
                            inputStream -> saveFile(inputStream, getFilenameFromUrlString(url)))).orElse(Mono.empty()))
                    .then(ok);
        } else if (content.map(s->s.startsWith("https://www.curseforge.com/minecraft/")).orElse(Boolean.FALSE)) {
            LogManager.getLogger().info("Downloading from CURSE URL: {}", content.get());
            files = downloadUrl("https://addons-ecs.forgesvc.net/api/v2/addon/0/file/"+ getFilenameFromUrlString(content.get())+"/download-url", this::getInputStreamAsString)
                    .flatMap(actual->downloadUrl(actual,
                            is-> saveFile(is, getFilenameFromUrlString(actual))))
                    .then(ok);
        } else {
            LogManager.getLogger().info("Unable to understand message: {}", content.orElse("NO CONTENT FOUND"));
            files = message.addReaction(UNAMUSED);
        }
        return files.onErrorResume(throwable -> reactionError(throwable, message));
    }

    private Mono<Void> reactionError(final Throwable throwable, final Message message) {
        LogManager.getLogger().catching(throwable);
        return message.addReaction(CROSS);
    }

    private String getInputStreamAsString(final InputStream is) {
        Scanner s = new Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }

    private String getFilenameFromUrlString(final String url) {
        return url.substring(url.lastIndexOf('/')+1).trim();
    }

    private String saveFile(final InputStream inputStream, final String filename) {
        try {
            LogManager.getLogger().info("Downloading file to {}", filename);
            Files.copy(inputStream, config.getModDir().resolve(filename));
            LogManager.getLogger().info("File download to {} complete", filename);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return filename;
    }

    private Mono<Message> buildCertificateAttachment(final Tuple3<InputStream, Member, TextChannel> tuple) {
        final TextChannel tc = tuple.getT3();
        final Member user = tuple.getT2();
        final InputStream cert = tuple.getT1();
        LogManager.getLogger().info("Generating certificate for {}", user.getUsername());
        return tc.createMessage(spec -> spec
                .addFile(user.getUsername() + ".pem", cert)
                .setContent(user.getMention() + " your certificate is here. Download and install it in your servermods folder.")
        );
    }

    private <T> Mono<T> downloadAttachment(final Attachment attachment, final Function<InputStream, T> inputStreamHandler) {
        LogManager.getLogger().info("Downloading from message attachment: {}", attachment.getFilename());
        return downloadUrl(attachment.getUrl(), inputStreamHandler);
    }

    private <T> Mono<T> downloadUrl(final String url, final Function<InputStream, T> inputStreamHandler) {
        LogManager.getLogger().info("Downloading file from URL {}", url);
        HttpClient httpClient = HttpClient.create().followRedirect(true)
                .headers(h->h.set(HttpHeaderNames.USER_AGENT, "SPL Discord Bot (1.0)").set(HttpHeaderNames.ACCEPT, "*/*"));
        return httpClient.get().uri(url).responseSingle((r, buf) -> handleDownload(r, url, buf, inputStreamHandler));
    }

    private <T> Mono<T> handleDownload(HttpClientResponse r, final String url, ByteBufMono buf, final Function<InputStream, T> inputStreamHandler) {
        if (r.status() == HttpResponseStatus.OK) {
            return buf.asInputStream().map(inputStreamHandler);
        } else {
            LogManager.getLogger().error("Failed to download {} : {}", url, r.status());
            return Mono.error(new IllegalStateException("Invalid status response "+r.status()));
        }
    }
}