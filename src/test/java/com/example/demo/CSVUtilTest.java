package com.example.demo;
import com.example.demo.service.PlayerService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.*;


class CSVUtilTest {

    @Autowired
    PlayerService playerService;

    @Test
    void converterData() {
        List<Player> list = CsvUtilFile.getPlayers();
        assert list.size() == 18207;
    }

    @Test
    void converterDatamongo(){
        Flux<Player> listFlux = playerService.listAll();
        var fluxLenght = listFlux.count();
        assert fluxLenght.block() == 18207;
    }

    @Test
    void reactive_filtrarJugadoresMayoresA35() {
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .buffer(100)
                .flatMap(playerA -> listFlux
                        .filter(playerB -> playerA.stream()
                                .anyMatch(a -> a.club.equals(playerB.club)))
                )
                .distinct()
                .collectMultimap(Player::getClub);

        assert listFilter.block().size() == 322;
    }

    @Test
    @DisplayName("Filtro jugadores mayores de 34 años")
    void reactive_filtrarJugadoresMayoresDe34() {
        List<Player> playerList = CsvUtilFile.getPlayers();
        Flux<Player> fluxList = Flux.fromStream(playerList.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = fluxList
                .filter(player -> player.age > 34)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .buffer(100)
                .flatMap(playerA -> fluxList
                        .filter(playerB -> playerA.stream()
                                .anyMatch(a -> a.club.equals(playerB.club)))
                )
                .distinct()
                .collectMultimap(Player::getClub);
        //System.out.println(listFilter.block().size());
        assert listFilter.block().size() == 322;
    }

    @Test
    @DisplayName("Filtro jugadores de Everton mayores de 34 años")
    void reactive_filtrarJugadoresDeEvertonMayoresDe34() {
        List<Player> playerList = CsvUtilFile.getPlayers();
        Flux<Player> fluxList = Flux.fromStream(playerList.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = fluxList
                .filter(player -> player.age >= 34 && player.club.equals("Everton"))
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                }).distinct()
                .collectMultimap(Player::getClub);
        listFilter.block().forEach((team, players) -> {
            System.out.println(team);
            players.stream().forEach(player1 -> System.out.println(player1.name + " " + player1.age));
            assert players.size() == 2;
        });
    }

    @Test
    @DisplayName("Filtro jugadores nacionalidad y ranking")
    void reactive_filtrarNacionalidadRanking() {
        List<Player> playerList = CsvUtilFile.getPlayers();
        Flux<Player> fluxList = Flux.fromStream(playerList.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = fluxList
                .buffer(100)
                .flatMap(playerA -> fluxList
                        .filter(playerB -> playerA.stream()
                                .anyMatch(a -> a.national.equals(playerB.national)))
                ).distinct()
                .sort((player1, player2) -> Math.max(player1.winners, player2.winners))
                .collectMultimap(Player::getNational);

        System.out.println("Cantidad paises -> " + listFilter.block().size());

        Objects.requireNonNull(listFilter.block()).forEach((country, players) -> {
            System.out.println("Pais: " + country);
            players.forEach(player -> {
                System.out.println(player.name + " victorias: " + player.winners);
            });
        });
        assert listFilter.block().size() == 164;
    }
}
