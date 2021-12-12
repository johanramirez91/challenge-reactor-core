package com.example.demo.controller;
import com.example.demo.CsvUtilFile;
import com.example.demo.Player;
import com.example.demo.service.PlayerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.util.List;

@RestController
@RequestMapping(value = "/list")
public class PlayerController {

    @Autowired
    PlayerService playerService;

    @GetMapping(value = "/pall")
    public List<Player> playerList(){
        return CsvUtilFile.getPlayers();
    }

    @GetMapping(value = "/all")
    public Flux<Player> list(){
        return playerService.listAll();
    }

}
