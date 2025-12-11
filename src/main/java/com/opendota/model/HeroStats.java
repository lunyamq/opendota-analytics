package com.opendota.model;

import java.io.Serializable;

public class HeroStats implements Serializable {
    private int id;
    private String name;
    private String localizedName;
    private int baseHealth;
    private int baseMana;
    private int baseAttackMin;
    private int baseAttackMax;
    private int moveSpeed;
    private String primaryAttr;
    private String[] roles;

    public HeroStats() { }

    public void setId(int id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setLocalizedName(String localizedName) {
        this.localizedName = localizedName;
    }

    public void setBaseHealth(int baseHealth) {
        this.baseHealth = baseHealth;
    }

    public void setBaseMana(int baseMana) {
        this.baseMana = baseMana;
    }

    public void setBaseAttackMin(int baseAttackMin) {
        this.baseAttackMin = baseAttackMin;
    }

    public void setBaseAttackMax(int baseAttackMax) {
        this.baseAttackMax = baseAttackMax;
    }

    public void setMoveSpeed(int moveSpeed) {
        this.moveSpeed = moveSpeed;
    }

    public void setPrimaryAttr(String primaryAttr) {
        this.primaryAttr = primaryAttr;
    }

    public void setRoles(String[] roles) {
        this.roles = roles;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getLocalizedName() {
        return localizedName;
    }

    public int getBaseHealth() {
        return baseHealth;
    }

    public int getBaseMana() {
        return baseMana;
    }

    public int getBaseAttackMin() {
        return baseAttackMin;
    }

    public int getBaseAttackMax() {
        return baseAttackMax;
    }

    public int getMoveSpeed() {
        return moveSpeed;
    }

    public String getPrimaryAttr() {
        return primaryAttr;
    }

    public String[] getRoles() {
        return roles;
    }
}